# main.py (fixed: do NOT start polling when webhook is active; robust webhook detection)
import os
import logging
import asyncio
import signal
from aiohttp import web, ClientSession

from aiogram.types import Update
from aiogram.utils.exceptions import TerminatedByOtherGetUpdates, CantGetUpdates

import bot_app  # imports bot, dp, scheduler, handlers are registered on import

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # e.g. https://financial-guide.onrender.com
PORT = int(os.environ.get("PORT", 10000))
FORCE_POLLING = os.environ.get("FORCE_POLLING", "0") == "1"
KEEP_BOT_ALIVE = os.environ.get("KEEP_BOT_ALIVE", "1") != "0"
PING_INTERVAL = int(os.environ.get("SELF_PING_INTERVAL", 25))

bot = bot_app.bot
dp = bot_app.dp
scheduler = bot_app.scheduler

_updates_queue: asyncio.Queue | None = None
_worker_task: asyncio.Task | None = None
_self_ping_task: asyncio.Task | None = None
_client_session: ClientSession | None = None

_shutdown_initiator = {"signal": None}

def _register_signal_handlers(loop: asyncio.AbstractEventLoop):
    def _on_signal(sig):
        _shutdown_initiator["signal"] = str(sig)
        logger.info("Process received signal: %s. Marking shutdown_initiator.", sig)
    try:
        loop.add_signal_handler(signal.SIGINT, _on_signal, signal.SIGINT)
        loop.add_signal_handler(signal.SIGTERM, _on_signal, signal.SIGTERM)
        logger.info("Signal handlers registered")
    except NotImplementedError:
        logger.debug("Signal handlers not supported on this platform")

# ---------------- worker / webhook handler ----------------
async def webhook_worker():
    logger.info("Webhook worker started")
    while True:
        try:
            update: Update = await _updates_queue.get()
            try:
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
    try:
        data = await request.json()
        update = Update(**data)
    except Exception:
        logger.exception("handle_webhook: invalid payload")
        return web.Response(status=400, text="invalid")
    try:
        _updates_queue.put_nowait(update)
    except asyncio.QueueFull:
        logger.warning("updates queue full - dropping incoming update")
    return web.Response(text="OK")

async def handle_root(request: web.Request):
    return web.Response(text="OK")

async def health_handler(request: web.Request):
    return web.Response(text="OK")

async def metrics_handler(request: web.Request):
    try:
        r = await bot_app.db_fetchone("SELECT COUNT(*) as c FROM users", ())
        users_count = int(r["c"]) if r else 0
        r = await bot_app.db_fetchone("SELECT COUNT(*) as c FROM expenses", ())
        expenses_count = int(r["c"]) if r else 0
        r = await bot_app.db_fetchone("SELECT COUNT(*) as c FROM pending", ())
        pending_count = int(r["c"]) if r else 0
        payload = {
            "users_count": users_count,
            "expenses_count": expenses_count,
            "pending_count": pending_count,
            "webhook_url_env": bool(WEBHOOK_URL),
            "force_polling": FORCE_POLLING,
            "keep_bot_alive": KEEP_BOT_ALIVE,
        }
        return web.json_response(payload)
    except Exception:
        logger.exception("metrics_handler failed")
        return web.json_response({"ok": False}, status=500)

# ---------------- self-ping ----------------
async def self_ping_loop(app: web.Application):
    global _client_session
    if _client_session is None:
        _client_session = ClientSession()
    session = _client_session
    target = WEBHOOK_URL.rstrip("/") if WEBHOOK_URL else f"http://localhost:{PORT}"
    logger.info("self_ping_loop started (interval=%s s) target=%s", PING_INTERVAL, target)
    try:
        while True:
            try:
                try:
                    await bot.get_me()
                except Exception as e:
                    logger.debug("self_ping: bot.get_me() failed: %s", e)
                try:
                    async with session.get(target, timeout=10) as resp:
                        logger.debug("self_ping: GET %s -> %s", target, resp.status)
                except Exception as e:
                    logger.debug("self_ping: public GET failed: %s", e)
                try:
                    local = f"http://localhost:{PORT}/healthz"
                    async with session.get(local, timeout=5) as resp:
                        logger.debug("self_ping: local healthz -> %s", resp.status)
                except Exception as e:
                    logger.debug("self_ping: local healthz failed: %s", e)
            except Exception:
                logger.exception("self_ping_loop encountered exception (continuing)")
            await asyncio.sleep(PING_INTERVAL)
    except asyncio.CancelledError:
        logger.info("self_ping_loop cancelled")
        raise

# ---------------- polling runner ----------------
async def polling_runner(app: web.Application):
    logger.info("Polling runner started")
    backoff = 1
    max_backoff = 60
    while True:
        try:
            logger.info("Starting dp.start_polling()")
            await dp.start_polling(timeout=20)
            logger.info("dp.start_polling() ended normally")
            break
        except asyncio.CancelledError:
            logger.info("Polling runner cancelled")
            raise
        except (TerminatedByOtherGetUpdates, CantGetUpdates) as e:
            # If Telegram says we can't use getUpdates because webhook is active OR another process,
            # stop polling to avoid log spam. Admin can enable FORCE_POLLING to explicitly delete webhook.
            logger.warning("Polling aborted due to Telegram conflict (%s). Stopping polling runner.", repr(e))
            break
        except Exception:
            logger.exception("Polling crashed, will retry after backoff")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)

# ---------------- startup / cleanup ----------------
async def on_startup_app(app: web.Application):
    global _updates_queue, _worker_task, _self_ping_task, _client_session
    logger.info("on_startup_app: initializing")

    # initialize bot_app (DB, scheduler, pending)
    await bot_app.init_app_for_runtime(app)

    # create updates queue + worker
    _updates_queue = asyncio.Queue(maxsize=2000)
    app['updates_queue'] = _updates_queue
    _worker_task = asyncio.create_task(webhook_worker())

    # client session for self ping
    _client_session = ClientSession()
    app['client_session'] = _client_session

    if KEEP_BOT_ALIVE:
        app['self_ping'] = asyncio.create_task(self_ping_loop(app))
        _self_ping_task = app['self_ping']
        logger.info("self_ping_loop started (KEEP_BOT_ALIVE=True)")

    app['keep_alive'] = asyncio.create_task(asyncio.sleep(3600*24))

    # DECISION: detect webhook state on Telegram BEFORE starting polling
    webhook_info = None
    try:
        webhook_info = await bot.get_webhook_info()
        # webhook_info.url exists and may be '' if none
    except Exception:
        logger.exception("get_webhook_info failed (continuing)")

    webhook_active = False
    webhook_url_on_telegram = None
    try:
        if webhook_info and getattr(webhook_info, "url", None):
            webhook_url_on_telegram = webhook_info.url
            if webhook_url_on_telegram:
                webhook_active = True
    except Exception:
        logger.debug("Unable to read webhook_info attributes (ignored)")

    logger.info("WEBHOOK env: %s | webhook on Telegram: %s", bool(WEBHOOK_URL), webhook_url_on_telegram)

    # If operator explicitly wants polling => remove webhook (delete_webhook) and start polling.
    if FORCE_POLLING:
        if webhook_active:
            try:
                await bot.delete_webhook(drop_pending_updates=True)
                logger.info("FORCE_POLLING: deleted existing webhook on Telegram")
            except Exception:
                logger.exception("FORCE_POLLING: failed to delete webhook (continuing)")
        app['polling_task'] = asyncio.create_task(polling_runner(app))
        logger.info("Polling mode enabled (FORCE_POLLING=True)")
        return

    # If webhook is active on Telegram -> use webhook mode (do not start polling)
    if webhook_active:
        logger.info("Detected webhook active on Telegram (%s). Running in webhook mode; not starting polling.", webhook_url_on_telegram)
        # Try to ensure webhook points to our WEBHOOK_URL if we have one configured (best-effort)
        if WEBHOOK_URL and webhook_url_on_telegram and not webhook_url_on_telegram.startswith(WEBHOOK_URL.rstrip("/")):
            # URLs differ — attempt to set webhook to the expected URL.
            webhook_try = WEBHOOK_URL.rstrip("/") + "/webhook"
            try:
                await bot.set_webhook(webhook_try)
                logger.info("Re-set webhook to configured WEBHOOK_URL: %s", webhook_try)
            except Exception:
                logger.exception("Failed to set webhook to WEBHOOK_URL (ignored)")
        return

    # No webhook active on Telegram:
    if WEBHOOK_URL:
        # Try to set webhook to our URL. If it succeeds -> webhook mode.
        webhook_try = WEBHOOK_URL.rstrip("/") + "/webhook"
        success = False
        for attempt in range(3):
            try:
                await bot.set_webhook(webhook_try)
                logger.info("Webhook set successfully: %s", webhook_try)
                success = True
                break
            except Exception:
                logger.exception("set_webhook failed (attempt %s)", attempt+1)
                await asyncio.sleep(1 + attempt*2)
        if success:
            logger.info("Webhook mode enabled (set_webhook succeeded)")
            return
        else:
            logger.warning("set_webhook attempts failed. Not starting polling because FORCE_POLLING is not set. Please inspect WEBHOOK_URL or set FORCE_POLLING=1 to force polling.")
            return
    else:
        # No WEBHOOK_URL configured -> fall back to polling
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            logger.debug("delete_webhook pre-polling failed (ignored)")
        app['polling_task'] = asyncio.create_task(polling_runner(app))
        logger.info("Polling mode enabled (no WEBHOOK_URL configured)")

async def on_cleanup_app(app: web.Application):
    global _updates_queue, _worker_task, _self_ping_task, _client_session
    logger.info("on_cleanup: starting cleanup (initiator=%s)", _shutdown_initiator.get("signal"))

    if app.get('keep_alive'):
        app['keep_alive'].cancel()
        try:
            await app['keep_alive']
        except Exception:
            pass

    if app.get('self_ping'):
        app['self_ping'].cancel()
        try:
            await app['self_ping']
        except Exception:
            pass

    if _client_session:
        try:
            await _client_session.close()
            logger.info("ClientSession closed")
        except Exception:
            logger.exception("Error while closing client session")
    _client_session = None

    if _worker_task:
        _worker_task.cancel()
        try:
            await _worker_task
        except Exception:
            pass

    if _updates_queue:
        try:
            await asyncio.wait_for(_updates_queue.join(), timeout=2.0)
        except Exception:
            pass
    _updates_queue = None

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

    # delete webhook only if NOT KEEP_BOT_ALIVE (same policy as before)
    if not KEEP_BOT_ALIVE:
        try:
            await bot.delete_webhook()
            logger.info("Webhook deleted on cleanup")
        except Exception:
            logger.debug("Failed to delete webhook on cleanup", exc_info=True)
    else:
        logger.info("KEEP_BOT_ALIVE=True -> skipping webhook deletion")

    try:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shutdown")
    except Exception:
        logger.debug("Scheduler shutdown failed", exc_info=True)

    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
        logger.info("Storage closed")
    except Exception:
        logger.debug("Storage close failed", exc_info=True)

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
            logger.info('Bot closed (client session closed)')
        except Exception:
            logger.exception('Error while closing bot')
    else:
        logger.info('KEEP_BOT_ALIVE=True -> skipping bot.close()')

    try:
        await bot_app.close_db()
    except Exception:
        logger.exception('aiosqlite close failed', exc_info=True)

    logger.info('Cleanup complete')

async def set_webhook_handler(request: web.Request):
    if not WEBHOOK_URL:
        return web.json_response({"ok": False, "error": "WEBHOOK_URL not configured"}, status=400)
    webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
    try:
        await bot.set_webhook(webhook)
        return web.json_response({"ok": True, "webhook": webhook})
    except Exception as e:
        logger.exception('set_webhook_handler failed')
        return web.json_response({"ok": False, "error": str(e)})

def create_app():
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_post('/webhook', handle_webhook)
    app.router.add_get('/healthz', health_handler)
    app.router.add_post('/set_webhook', set_webhook_handler)
    app.router.add_get('/debug', metrics_handler)
    app.router.add_get('/metrics', metrics_handler)
    app.on_startup.append(on_startup_app)
    app.on_cleanup.append(on_cleanup_app)
    return app

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    _register_signal_handlers(loop)
    app = create_app()
    logger.info("Starting web app on 0.0.0.0:%s (FORCE_POLLING=%s, KEEP_BOT_ALIVE=%s)", PORT, FORCE_POLLING, KEEP_BOT_ALIVE)
    web.run_app(app, host='0.0.0.0', port=PORT)
