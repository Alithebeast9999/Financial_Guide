# main.py
import os
import logging
import asyncio
import signal
import inspect
import aiohttp
from aiohttp import web

from aiogram.utils.exceptions import TerminatedByOtherGetUpdates

import bot_app  # <-- imports bot, dp, scheduler, handlers are registered on import

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # e.g. https://financial-guide.onrender.com
PORT = int(os.environ.get("PORT", 10000))
FORCE_POLLING = os.environ.get("FORCE_POLLING", "0") == "1"
KEEP_BOT_ALIVE = os.environ.get("KEEP_BOT_ALIVE", "1") != "0"

# Shared references
bot = bot_app.bot
dp = bot_app.dp
scheduler = bot_app.scheduler

# Runtime artifacts
_updates_queue = None
_worker_task = None

async def webhook_worker():
    logger.info("Webhook worker started")
    try:
        try:
            # prefer instance method; fallback to class method
            bot.set_current(bot)
        except Exception:
            try:
                bot_app.Bot.set_current(bot)
            except Exception:
                logger.debug("Bot.set_current not available")
    except Exception:
        logger.debug("Bot.set_current failed in worker")
    while True:
        try:
            update = await _updates_queue.get()
            try:
                try:
                    bot.set_current(bot)
                except Exception:
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
    try:
        data = await request.json()
        update = bot_app.TgUpdate.to_object(data)
    except Exception:
        logger.exception("handle_webhook: invalid payload")
        return web.Response(status=400, text="invalid")
    try:
        _updates_queue.put_nowait(update)
    except asyncio.QueueFull:
        logger.warning("queue full - drop update")
    return web.Response(text="OK")

async def handle_root(request: web.Request):
    return web.Response(text="OK")

async def keep_alive_ping():
    try:
        while True:
            try:
                await bot.get_me()
            except Exception:
                logger.debug("keep_alive_ping: bot.get_me() failed", exc_info=True)
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        logger.info("keep_alive_ping cancelled")
        raise

async def ensure_no_webhook(max_retries=6, delay=1.0):
    """Try to ensure webhook is removed before polling — reduces conflicts."""
    for attempt in range(1, max_retries + 1):
        try:
            wh = await bot.get_webhook_info()
            url = None
            try:
                url = getattr(wh, "url", None)
                if url is None and hasattr(wh, "to_python"):
                    url = wh.to_python().get("url")
            except Exception:
                url = None
            if not url:
                logger.info("No webhook set (checked attempt %d).", attempt)
                return True
            logger.info("Webhook present: %s (attempt %d) — deleting", url, attempt)
            try:
                await bot.delete_webhook(drop_pending_updates=True)
                logger.info("Webhook deleted (attempt %d)", attempt)
            except Exception:
                logger.exception("delete_webhook failed (attempt %d)", attempt)
        except Exception:
            logger.exception("Failed to check/delete webhook (attempt %d)", attempt)
        await asyncio.sleep(delay)
        delay = min(delay * 2, 5.0)
    logger.warning("ensure_no_webhook: reached max retries; continuing anyway")
    return False

async def polling_runner(app):
    backoff = 1
    max_backoff = 60
    logger.info("Polling runner started")
    while True:
        try:
            try:
                bot.set_current(bot)
            except Exception:
                try:
                    bot_app.Bot.set_current(bot)
                except Exception:
                    pass
            logger.info("Starting dp.start_polling()")
            try:
                await ensure_no_webhook()
            except Exception:
                logger.debug("ensure_no_webhook failed (ignored)", exc_info=True)
            await dp.start_polling(timeout=20)
            logger.info("dp.start_polling() ended normally")
            break
        except asyncio.CancelledError:
            logger.info("Polling runner cancelled")
            raise
        except TerminatedByOtherGetUpdates:
            logger.warning(
                "TerminatedByOtherGetUpdates — another getUpdates exists. "
                "Attempting webhook deletion and sleeping before retry."
            )
            try:
                await bot.delete_webhook(drop_pending_updates=True)
                logger.info("delete_webhook called to recover from TerminatedByOtherGetUpdates")
            except Exception:
                logger.debug("delete_webhook during recovery failed", exc_info=True)
            await asyncio.sleep(60)
            backoff = 1
        except Exception:
            logger.exception("Polling crashed, will retry")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)

_shutdown_initiator = {"signal": None}

def _register_signal_handlers(loop):
    def _on_signal(sig):
        _shutdown_initiator["signal"] = str(sig)
        logger.info(f"Process received signal: {sig}. Marking shutdown_initiator.")
    try:
        loop.add_signal_handler(signal.SIGINT, _on_signal, signal.SIGINT)
        loop.add_signal_handler(signal.SIGTERM, _on_signal, signal.SIGTERM)
        logger.info("Signal handlers registered")
    except NotImplementedError:
        logger.debug("Signal handlers not supported on this platform")

async def _close_if_session(obj, name_hint=None):
    """Close object if it's an aiohttp.ClientSession (async)."""
    if isinstance(obj, aiohttp.ClientSession):
        try:
            await obj.close()
            logger.info("Closed aiohttp.ClientSession %s", name_hint or "")
            return True
        except Exception:
            logger.exception("Failed to close aiohttp.ClientSession %s", name_hint or "")
    return False

async def on_startup_app(app):
    global _updates_queue, _worker_task
    logger.info("on_startup_app: initializing")

    # Initialize bot_app internals (db_lock, scheduler jobs, etc.)
    init_fn = getattr(bot_app, "init_app_for_runtime", None)
    if callable(init_fn):
        try:
            await init_fn(app)
        except Exception:
            logger.exception("bot_app.init_app_for_runtime failed (continuing)")
    else:
        logger.warning("bot_app.init_app_for_runtime not found — skipping optional bot_app runtime init")

    # Queue + worker for webhook handling
    _updates_queue = asyncio.Queue(maxsize=2000)
    app['updates_queue'] = _updates_queue
    _worker_task = asyncio.create_task(webhook_worker())

    # obtain and store bot session inside async context to close later
    try:
        sess = await bot.get_session()
        # ensure it's aiohttp.ClientSession
        if isinstance(sess, aiohttp.ClientSession):
            app['bot_session'] = sess
            logger.info("Bot session ready and saved to app['bot_session']")
        else:
            # If get_session returned something else, still keep it for inspection
            app['bot_session'] = sess
            logger.info("Bot.get_session returned object (saved) — type=%s", type(sess))
    except Exception:
        logger.exception("Failed to get bot session on startup (non-fatal)")

    # start keep-alive ping if requested
    if KEEP_BOT_ALIVE:
        app['keep_alive_ping'] = asyncio.create_task(keep_alive_ping())
        logger.info("keep_alive_ping started (KEEP_BOT_ALIVE=True)")

    # small dummy to keep loop busy (optional)
    app['keep_alive'] = asyncio.create_task(asyncio.sleep(3600*24))

    # choose mode
    if FORCE_POLLING or KEEP_BOT_ALIVE or not WEBHOOK_URL:
        try:
            await ensure_no_webhook(max_retries=6)
            logger.info("Ensuring no webhook is set before polling (max_retries=6)")
        except Exception:
            logger.debug("ensure_no_webhook raised", exc_info=True)
        app['polling_task'] = asyncio.create_task(polling_runner(app))
        logger.info("Polling mode enabled")
    else:
        webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
        success = False
        for attempt in range(3):
            try:
                await bot.set_webhook(webhook)
                logger.info("Webhook set successfully: %s", webhook)
                success = True
                break
            except Exception:
                logger.exception("set_webhook failed (attempt %s)", attempt + 1)
                await asyncio.sleep(1 + attempt * 2)
        if not success:
            logger.warning("set_webhook failed after retries - falling back to polling")
            try:
                await bot.delete_webhook(drop_pending_updates=True)
            except Exception:
                logger.debug("delete_webhook during fallback failed")
            app['polling_task'] = asyncio.create_task(polling_runner(app))

async def on_cleanup_app(app):
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
            try:
                await dp.stop_polling()
            except Exception:
                logger.debug("dp.stop_polling() failed or not available", exc_info=True)
        except Exception:
            logger.debug("Error while stopping polling", exc_info=True)
        polling_task.cancel()
        try:
            await polling_task
        except Exception:
            pass

    # try to delete webhook (best-effort)
    try:
        await bot.delete_webhook()
        logger.info("Webhook deleted on cleanup (best-effort)")
    except Exception:
        logger.debug("Failed to delete webhook on cleanup (ignored)", exc_info=True)

    # shutdown scheduler
    try:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shutdown")
    except Exception:
        logger.debug("Scheduler shutdown failed", exc_info=True)

    # close storage
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
        logger.info("Storage closed")
    except Exception:
        logger.debug("Storage close failed", exc_info=True)

    # --- Aggressive session cleanup (fix Unclosed client session) ---
    # 1) close saved app['bot_session'] if it exists
    try:
        bot_sess = app.get('bot_session')
        closed_any = False
        if bot_sess:
            if isinstance(bot_sess, aiohttp.ClientSession):
                try:
                    await bot_sess.close()
                    logger.info("Closed app['bot_session']")
                    closed_any = True
                except Exception:
                    logger.exception("Failed to close app['bot_session']")
            else:
                logger.info("app['bot_session'] exists but is not aiohttp.ClientSession: %s", type(bot_sess))
    except Exception:
        logger.exception("Error while closing app['bot_session']")

    # 2) try to find session objects attached to bot instance (common names)
    try:
        session_attrs = ['session', '_session', 'client_session', '_client_session', 'sess', '_sess']
        for attr in session_attrs:
            sess = getattr(bot, attr, None)
            if isinstance(sess, aiohttp.ClientSession):
                try:
                    await sess.close()
                    logger.info("Closed bot.%s session", attr)
                    closed_any = True
                except Exception:
                    logger.exception("Failed to close bot.%s", attr)
    except Exception:
        logger.exception("Error while inspecting bot for session attributes")

    # 3) search bot_app module for any ClientSession instances (best-effort)
    try:
        for name, val in vars(bot_app).items():
            if isinstance(val, aiohttp.ClientSession):
                try:
                    await val.close()
                    logger.info("Closed bot_app.%s ClientSession", name)
                    closed_any = True
                except Exception:
                    logger.exception("Failed to close bot_app.%s ClientSession", name)
    except Exception:
        logger.debug("Scanning bot_app for sessions failed", exc_info=True)

    # 4) finally call bot.close() which should close internal session(s)
    try:
        await bot.close()
        logger.info('Bot closed (await bot.close())')
    except Exception:
        logger.exception('Error while awaiting bot.close()')

    # 5) after bot.close() try again to find stray aiohttp sessions in globals (last resort)
    try:
        # walk module globals in current process (limited check) - do not over-scan to avoid noise
        possible = []
        mod_names = ['bot_app']
        for modname in mod_names:
            mod = globals().get(modname)
            if mod:
                for k, v in getattr(mod, '__dict__', {}).items():
                    if isinstance(v, aiohttp.ClientSession):
                        try:
                            await v.close()
                            logger.info("Closed %s.%s ClientSession (post bot.close)", modname, k)
                            closed_any = True
                        except Exception:
                            logger.exception("Failed to close %s.%s ClientSession", modname, k)
    except Exception:
        logger.debug("Final scan for ClientSession failed", exc_info=True)

    if not closed_any:
        logger.info("No aiohttp.ClientSession explicitly closed in cleanup (either already closed or none found).")

    # close sqlite connection
    try:
        bot_app.conn.close()
        logger.info('DB connection closed')
    except Exception:
        logger.debug('DB close failed', exc_info=True)

    logger.info('Cleanup complete')

# Admin endpoints
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

async def debug_handler(request: web.Request):
    info = {
        'queue_size': _updates_queue.qsize() if _updates_queue else None,
        'worker_running': _worker_task is not None and not _worker_task.done(),
        'scheduler_running': getattr(scheduler, 'running', None),
        'force_polling': FORCE_POLLING,
        'keep_bot_alive': KEEP_BOT_ALIVE,
        'webhook_url_env': WEBHOOK_URL,
    }
    try:
        wh = await bot.get_webhook_info()
        info['telegram_webhook'] = wh.to_python() if wh else None
    except Exception as e:
        info['telegram_webhook_error'] = str(e)
    return web.json_response(info)

def create_app():
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_post('/webhook', handle_webhook)
    app.router.add_post('/set_webhook', set_webhook_handler)
    app.router.add_get('/debug', debug_handler)
    app.on_startup.append(on_startup_app)
    app.on_cleanup.append(on_cleanup_app)
    return app

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    _register_signal_handlers(loop)
    app = create_app()
    logger.info(f"Starting web app on 0.0.0.0:{PORT} (FORCE_POLLING={FORCE_POLLING}, KEEP_BOT_ALIVE={KEEP_BOT_ALIVE})")
    web.run_app(app, host='0.0.0.0', port=PORT)
