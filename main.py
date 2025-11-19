# main.py
"""
Main entrypoint — robust webhook/polling launcher for Financial_Guide.

Safe improvements added:
 - optional secret-token check for webhook requests (HOOK_SECRET / X-Telegram-Bot-Api-Secret-Token)
 - resilient set_webhook with retries and jitter
 - /ready endpoint which checks bot.get_me() and a light DB ping (via executor)
 - registers both /webhook and /webhook/{token} endpoints (compatibility)
 - better logging and defensive shutdown handling
"""
import os
import logging
import asyncio
import signal
import json
import random
from typing import Optional

from aiohttp import web
from aiogram.utils.exceptions import TerminatedByOtherGetUpdates

import bot_app  # imports bot, dp, scheduler, registers handlers on import

logger = logging.getLogger("main")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Config from env
PORT = int(os.environ.get("PORT", "10000"))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # e.g. https://domain.com
WEBHOOK_PREFIX = os.environ.get("WEBHOOK_PREFIX", "/webhook").rstrip("/")
WEBHOOK_AUTO_DELETE = os.environ.get("WEBHOOK_AUTO_DELETE", "0") in ("1", "true", "yes")
FORCE_POLLING = os.environ.get("FORCE_POLLING", "0") == "1"
KEEP_BOT_ALIVE = os.environ.get("KEEP_BOT_ALIVE", "1") != "0"
HOOK_SECRET = os.environ.get("HOOK_SECRET")  # optional secret token to validate incoming webhooks

# Short aliases to objects exported by bot_app
bot = getattr(bot_app, "bot", None)
dp = getattr(bot_app, "dp", None)
scheduler = getattr(bot_app, "scheduler", None)

# runtime artifacts
_updates_queue: Optional[asyncio.Queue] = None
_worker_task: Optional[asyncio.Task] = None

# ---------- Helpers ----------

def _check_secret_header(request: web.Request) -> bool:
    """
    If HOOK_SECRET is configured, require header 'X-Telegram-Bot-Api-Secret-Token'
    to match. Otherwise allow.
    """
    if not HOOK_SECRET:
        return True
    header = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if not header:
        logger.warning("Webhook request missing secret header")
        return False
    if header != HOOK_SECRET:
        logger.warning("Webhook request provided wrong secret token")
        return False
    return True

async def set_webhook_with_retries(bot_obj, webhook_url: str, attempts: int = 3, base_sleep: float = 1.0) -> bool:
    """Try to call bot.set_webhook(webhook_url) with retries and jitter."""
    for attempt in range(1, attempts + 1):
        try:
            await bot_obj.set_webhook(webhook_url)
            logger.info("Webhook set successfully: %s", webhook_url)
            return True
        except Exception as ex:
            logger.warning("set_webhook attempt %s failed: %s", attempt, ex)
            if attempt == attempts:
                break
            jitter = random.random() * 0.3
            await asyncio.sleep(base_sleep * attempt + jitter)
    logger.error("set_webhook failed after %s attempts", attempts)
    return False

async def _db_ping(loop=None) -> bool:
    """
    Light DB ping executed in executor to avoid blocking event loop.
    Returns True if DB exists and responds to a simple query.
    """
    try:
        def ping():
            try:
                conn = getattr(bot_app, "conn", None)
                if not conn:
                    return False
                cur = conn.cursor()
                cur.execute("SELECT 1")
                _ = cur.fetchone()
                return True
            except Exception:
                return False
        if loop is None:
            loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, ping)
    except Exception:
        return False

# ---------- Worker & webhook queue ----------

async def webhook_worker():
    """Background worker that takes Updates from queue and calls dp.process_update"""
    global _updates_queue
    logger.info("Webhook worker started")
    # prefer to set current bot object for aiogram internals
    try:
        bot_app.Bot.set_current(bot)
    except Exception:
        logger.debug("Bot.set_current failed in worker (ignored)")
    while True:
        try:
            update = await _updates_queue.get()
            try:
                try:
                    bot_app.Bot.set_current(bot)
                except Exception:
                    pass
                await dp.process_update(update)
            except Exception:
                logger.exception("Error while processing update (worker)")
            finally:
                _updates_queue.task_done()
        except asyncio.CancelledError:
            logger.info("Webhook worker cancelled")
            break
        except Exception:
            logger.exception("Unexpected exception in webhook worker; continuing")

async def handle_webhook(request: web.Request):
    """
    Accepts Telegram webhook POSTs. Two route variants are supported:
      - /webhook
      - /webhook/{token}
    Payload is converted to aiogram Update and queued for processing.
    Secret header is checked if HOOK_SECRET configured.
    """
    if not _check_secret_header(request):
        return web.Response(status=403, text="forbidden")

    global _updates_queue
    if _updates_queue is None:
        logger.warning("Webhook received but updates queue not ready -> 503")
        return web.Response(status=503, text="service not ready")

    try:
        data = await request.json()
    except Exception:
        logger.exception("Failed to parse JSON from webhook request")
        # return 200 to tell Telegram we accepted the request (to avoid retries flooding)
        return web.Response(status=200, text="ok")

    try:
        upd = bot_app.TgUpdate.to_object(data)
    except Exception:
        logger.exception("Failed to construct Update from payload")
        return web.Response(status=200, text="ok")

    try:
        _updates_queue.put_nowait(upd)
    except asyncio.QueueFull:
        logger.warning("Updates queue full: dropping incoming update")
    return web.Response(text="OK")

async def handle_root(request: web.Request):
    return web.Response(text="OK")

# ---------- keep alive / polling runner ----------

async def keep_alive_ping():
    """Periodically call Telegram API to keep session healthy (only if KEEP_BOT_ALIVE)."""
    try:
        while True:
            try:
                await bot.get_me()
            except Exception:
                logger.debug("keep_alive_ping: bot.get_me() failed", exc_info=True)
            await asyncio.sleep(300)
    except asyncio.CancelledError:
        logger.info("keep_alive_ping cancelled")
        raise

async def polling_runner(app):
    """Resilient polling loop (if polling used)."""
    backoff = 1
    max_backoff = 60
    logger.info("Polling runner started")
    while True:
        try:
            try:
                bot_app.Bot.set_current(bot)
            except Exception:
                pass
            logger.info("Starting dp.start_polling()")
            await dp.start_polling()
            logger.info("dp.start_polling() ended normally")
            break
        except asyncio.CancelledError:
            logger.info("Polling runner cancelled")
            raise
        except TerminatedByOtherGetUpdates:
            logger.warning("TerminatedByOtherGetUpdates — another getUpdates exists. Sleeping 60s then retry.")
            await asyncio.sleep(60)
            backoff = 1
        except Exception:
            logger.exception("Polling crashed, will retry")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)

# ---------- startup / cleanup ----------

_shutdown_initiator = {"signal": None}

def _register_signal_handlers(loop):
    def _on_signal(sig):
        _shutdown_initiator["signal"] = str(sig)
        logger.info("Process received signal: %s. Marking shutdown initiator.", sig)
    try:
        loop.add_signal_handler(signal.SIGINT, _on_signal, signal.SIGINT)
        loop.add_signal_handler(signal.SIGTERM, _on_signal, signal.SIGTERM)
        logger.info("Signal handlers registered")
    except NotImplementedError:
        logger.debug("Signal handlers not supported on this platform")

async def on_startup_app(app: web.Application):
    global _updates_queue, _worker_task
    logger.info("on_startup_app: initializing")

    # Let bot_app initialize its runtime (db_lock, scheduler jobs, etc.)
    try:
        init_fn = getattr(bot_app, "init_app_for_runtime", None)
        if callable(init_fn):
            await init_fn(app)
        else:
            logger.debug("bot_app.init_app_for_runtime not present (skipping)")
    except Exception:
        logger.exception("bot_app.init_app_for_runtime failed (continuing)")

    # init queue + worker
    _updates_queue = asyncio.Queue(maxsize=2000)
    app['updates_queue'] = _updates_queue
    _worker_task = asyncio.create_task(webhook_worker())

    # obtain bot session in async context
    try:
        sess = await bot.get_session()
        app['bot_session'] = sess
        logger.info("Bot session ready")
    except Exception:
        logger.exception("Failed to get bot session on startup (non-fatal)")

    # keep-alive and dummy keep alive to keep loop occupied if needed
    if KEEP_BOT_ALIVE:
        app['keep_alive_ping'] = asyncio.create_task(keep_alive_ping())
        logger.info("keep_alive_ping started (KEEP_BOT_ALIVE=True)")
    app['keep_alive'] = asyncio.create_task(asyncio.sleep(3600*24))

    # decide mode: polling or webhook
    if FORCE_POLLING or KEEP_BOT_ALIVE or not WEBHOOK_URL:
        # prefer polling
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook deleted (pre-polling cleanup)")
        except Exception:
            logger.debug("delete_webhook pre-polling failed (ignored)")
        app['polling_task'] = asyncio.create_task(polling_runner(app))
        logger.info("Polling mode enabled")
    else:
        webhook = WEBHOOK_URL.rstrip("/") + WEBHOOK_PREFIX
        # Attempt resilient set_webhook (with retries)
        success = await set_webhook_with_retries(bot, webhook, attempts=3, base_sleep=1.0)
        if success:
            app['webhook_set_by_main'] = True
            app['webhook_full_url'] = webhook
        else:
            logger.warning("set_webhook failed, falling back to polling")
            try:
                await bot.delete_webhook(drop_pending_updates=True)
            except Exception:
                logger.debug("delete_webhook during fallback failed")
            app['polling_task'] = asyncio.create_task(polling_runner(app))

async def on_cleanup_app(app: web.Application):
    global _updates_queue, _worker_task
    logger.info("on_cleanup: starting cleanup (initiator=%s)", _shutdown_initiator.get("signal"))

    # cancel keep_alive dummy
    if app.get('keep_alive'):
        app['keep_alive'].cancel()
        try:
            await app['keep_alive']
        except Exception:
            pass

    # cancel keep_alive_ping
    if app.get('keep_alive_ping'):
        app['keep_alive_ping'].cancel()
        try:
            await app['keep_alive_ping']
        except Exception:
            pass

    # cancel worker
    if _worker_task:
        _worker_task.cancel()
        try:
            await _worker_task
        except Exception:
            pass

    # drain queue shortly
    if _updates_queue:
        try:
            await asyncio.wait_for(_updates_queue.join(), timeout=2.0)
        except Exception:
            pass
    _updates_queue = None

    # stop polling if active
    polling_task = app.get('polling_task')
    if polling_task:
        try:
            await dp.stop_polling()
        except Exception:
            logger.debug("dp.stop_polling() failed or not available", exc_info=True)
        polling_task.cancel()
        try:
            await polling_task
        except Exception:
            pass

    # delete webhook only if WEBHOOK_AUTO_DELETE is true
    if app.get('webhook_set_by_main') and WEBHOOK_AUTO_DELETE:
        try:
            await bot.delete_webhook()
            logger.info("Webhook deleted on cleanup")
        except Exception:
            logger.debug("Failed to delete webhook on cleanup", exc_info=True)
    else:
        if app.get('webhook_set_by_main'):
            logger.info("Webhook set by main but WEBHOOK_AUTO_DELETE=False -> left intact")

    # shutdown scheduler safely (try bot_app helper first, else fallback)
    try:
        stop_sched_fn = getattr(bot_app, "_stop_scheduler_if_any", None)
        if callable(stop_sched_fn):
            try:
                await stop_sched_fn()
            except Exception:
                logger.debug("bot_app._stop_scheduler_if_any raised", exc_info=True)
        else:
            sched = getattr(bot_app, "scheduler", None)
            if sched:
                try:
                    sched.shutdown(wait=False)
                    logger.info("Scheduler shutdown called")
                except Exception:
                    logger.debug("Direct scheduler.shutdown failed", exc_info=True)
    except Exception:
        logger.debug("Scheduler shutdown encountered issues", exc_info=True)

    # close storage
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
        logger.info("Storage closed")
    except Exception:
        logger.debug("Storage close failed", exc_info=True)

    # close bot session & bot only if NOT KEEP_BOT_ALIVE
    if not KEEP_BOT_ALIVE:
        try:
            sess = None
            try:
                sess = await bot.get_session()
            except Exception:
                sess = getattr(bot, 'session', None)
            if sess:
                try:
                    await sess.close()
                    logger.info('bot.session closed explicitly')
                except Exception:
                    logger.exception('Error while closing bot session')
        except Exception:
            logger.exception('Failed while closing bot session')
        try:
            await bot.close()
            logger.info('Bot closed')
        except Exception:
            logger.exception('Error while closing bot')
    else:
        logger.info('KEEP_BOT_ALIVE=True -> skipping bot.close()')

    # close sqlite connection
    try:
        if getattr(bot_app, "conn", None):
            bot_app.conn.close()
            logger.info('DB connection closed')
    except Exception:
        logger.debug('DB close failed', exc_info=True)

    logger.info("Cleanup complete")

# ---------- admin / health endpoints ----------

async def set_webhook_handler(request: web.Request):
    if not WEBHOOK_URL:
        return web.json_response({"ok": False, "error": "WEBHOOK_URL not configured"}, status=400)
    webhook = WEBHOOK_URL.rstrip("/") + WEBHOOK_PREFIX
    success = await set_webhook_with_retries(bot, webhook, attempts=3)
    if success:
        # mark for cleanup logic
        app = request.app
        app['webhook_set_by_main'] = True
        app['webhook_full_url'] = webhook
    return web.json_response({"ok": success, "webhook": webhook})

async def debug_handler(request: web.Request):
    info = {
        "queue_size": _updates_queue.qsize() if _updates_queue else None,
        "worker_running": _worker_task is not None and not _worker_task.done(),
        "scheduler_running": getattr(scheduler, "running", None),
        "force_polling": FORCE_POLLING,
        "keep_bot_alive": KEEP_BOT_ALIVE,
        "webhook_url_env": WEBHOOK_URL,
    }
    try:
        wh = await bot.get_webhook_info()
        info["telegram_webhook"] = wh.to_python() if wh else None
    except Exception as e:
        info["telegram_webhook_error"] = str(e)
    return web.json_response(info)

async def ready_handler(request: web.Request):
    """
    /ready endpoint:
     - checks bot.get_me()
     - performs a light DB ping (via executor) to ensure DB is open
    """
    ready = {"bot": False, "db": False}
    try:
        me = await bot.get_me()
        ready["bot"] = bool(me)
    except Exception:
        ready["bot"] = False
    try:
        loop = asyncio.get_running_loop()
        ready["db"] = await _db_ping(loop)
    except Exception:
        ready["db"] = False
    status = 200 if ready["bot"] or ready["db"] else 503
    return web.json_response(ready, status=status)

# ---------- create app & run ----------

def create_app():
    a = web.Application()
    a.router.add_get("/", handle_root)
    a.router.add_get("/ready", ready_handler)
    # both forms for compatibility
    a.router.add_post(f"{WEBHOOK_PREFIX}", handle_webhook)
    a.router.add_post(f"{WEBHOOK_PREFIX}/{{token:.*}}", handle_webhook)
    a.router.add_post("/set_webhook", set_webhook_handler)
    a.router.add_get("/debug", debug_handler)
    a.on_startup.append(on_startup_app)
    a.on_cleanup.append(on_cleanup_app)
    return a

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    _register_signal_handlers(loop)
    app = create_app()
    logger.info("Starting web app on 0.0.0.0:%s (FORCE_POLLING=%s, KEEP_BOT_ALIVE=%s)", PORT, FORCE_POLLING, KEEP_BOT_ALIVE)
    web.run_app(app, host="0.0.0.0", port=PORT)
