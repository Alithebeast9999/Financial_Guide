# main.py — hardened against TerminatedByOtherGetUpdates and session leaks
import os
import logging
import asyncio
import signal
from aiohttp import web

from aiogram.utils.exceptions import TerminatedByOtherGetUpdates

import bot_app  # contains bot, dp, scheduler, handlers

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 10000))
FORCE_POLLING = os.environ.get("FORCE_POLLING", "0") == "1"
KEEP_BOT_ALIVE = os.environ.get("KEEP_BOT_ALIVE", "1") != "0"

bot = bot_app.bot
dp = bot_app.dp
scheduler = bot_app.scheduler
conn = getattr(bot_app, "conn", None)

# runtime
_updates_queue = None
_worker_task = None

# If conflict happens, set this True to avoid tight-looping retries
_polling_conflict_detected = False

async def webhook_worker():
    logger.info("Webhook worker started")
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
    try:
        data = await request.json()
        update = bot_app.TgUpdate.to_object(data)
    except Exception:
        logger.exception("handle_webhook: invalid payload")
        return web.Response(status=400, text="invalid")
    try:
        _updates_queue.put_nowait(update)
    except asyncio.QueueFull:
        logger.warning("Updates queue full — dropping update")
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

async def ensure_no_webhook_before_polling(max_retries: int = 6, base_sleep: float = 0.5) -> bool:
    logger.info("Ensuring no webhook is set before polling (max_retries=%s)", max_retries)
    for attempt in range(1, max_retries + 1):
        try:
            wh = await bot.get_webhook_info()
            url = ""
            if wh:
                url = getattr(wh, "url", None) or (wh.to_python().get("url") if hasattr(wh, "to_python") else "")
            if not url:
                logger.info("No webhook set (checked attempt %s).", attempt)
                return True
            try:
                await bot.delete_webhook(drop_pending_updates=True)
                logger.info("Requested webhook deletion (attempt %s).", attempt)
            except Exception:
                logger.debug("delete_webhook attempt failed (attempt %s).", attempt, exc_info=True)
        except Exception:
            logger.exception("Failed to check/delete webhook (attempt %s).", attempt)
        await asyncio.sleep(base_sleep * attempt)
    # final check
    try:
        wh = await bot.get_webhook_info()
        url = ""
        if wh:
            url = getattr(wh, "url", None) or (wh.to_python().get("url") if hasattr(wh, "to_python") else "")
        if not url:
            logger.info("No webhook set after retries.")
            return True
        logger.warning("Webhook still present after retries: %s", url)
        return False
    except Exception:
        logger.exception("Final webhook check failed")
        return False

async def _close_bot_sessions():
    """
    Robustly close any aiohttp ClientSession attached to the bot.
    """
    try:
        # preferred: await bot.get_session() (aiogram API)
        sess = None
        try:
            sess = await bot.get_session()
        except Exception:
            sess = getattr(bot, 'session', None) or getattr(bot, '_session', None) or getattr(bot, '__session', None)
        if sess:
            # if it's an aiohttp.ClientSession, close it
            try:
                if hasattr(sess, "closed"):
                    if not sess.closed:
                        await sess.close()
                        logger.info("Closed bot aiohttp session (await).")
                else:
                    # fallback: try calling close (may be coroutine)
                    res = sess.close()
                    if asyncio.iscoroutine(res):
                        await res
                        logger.info("Closed bot aiohttp session (coroutine close).")
            except Exception:
                logger.exception("Error while closing bot session.")
    except Exception:
        logger.exception("Unexpected error when trying to close bot sessions.")

async def polling_runner(app):
    """
    Start polling. If TerminatedByOtherGetUpdates is raised, mark conflict and stop (no tight retry).
    """
    global _polling_conflict_detected
    logger.info("Polling runner started")
    try:
        try:
            bot_app.Bot.set_current(bot)
        except Exception:
            pass
        logger.info("Starting dp.start_polling()")
        await dp.start_polling(timeout=20)
        logger.info("dp.start_polling() ended normally")
    except asyncio.CancelledError:
        logger.info("Polling runner cancelled")
        raise
    except TerminatedByOtherGetUpdates:
        logger.error("TerminatedByOtherGetUpdates detected — another getUpdates client or webhook is active. Stopping polling runner.")
        _polling_conflict_detected = True
        # Try to set webhook if configured (best-effort)
        if WEBHOOK_URL:
            try:
                webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
                await bot.set_webhook(webhook)
                logger.info("Attempted to set webhook automatically: %s", webhook)
            except Exception:
                logger.exception("Automatic set_webhook failed (ignored).")
        # Ensure we close sessions proactively to avoid leaks
        try:
            await _close_bot_sessions()
        except Exception:
            logger.exception("Error while closing sessions after TerminatedByOtherGetUpdates.")
        return
    except Exception:
        logger.exception("Polling crashed unexpectedly; entering retry loop with backoff.")
        backoff = 1
        max_backoff = 60
        while True:
            try:
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)
                try:
                    bot_app.Bot.set_current(bot)
                except Exception:
                    pass
                await dp.start_polling(timeout=20)
                break
            except asyncio.CancelledError:
                raise
            except TerminatedByOtherGetUpdates:
                logger.error("TerminatedByOtherGetUpdates detected during retry — stopping polling runner.")
                _polling_conflict_detected = True
                if WEBHOOK_URL:
                    try:
                        webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
                        await bot.set_webhook(webhook)
                        logger.info("Attempted to set webhook automatically: %s", webhook)
                    except Exception:
                        logger.exception("Automatic set_webhook failed (ignored).")
                try:
                    await _close_bot_sessions()
                except Exception:
                    logger.exception("Error while closing sessions after conflict.")
                return
            except Exception:
                logger.exception("Retry of dp.start_polling failed; will try again.")

    logger.info("Polling runner finished (exiting)")

_shutdown_initiator = {"signal": None}

def _register_signal_handlers(loop):
    def _on_signal(sig):
        _shutdown_initiator["signal"] = str(sig)
        logger.info("Process received signal: %s. Marking shutdown_initiator.", sig)
    try:
        loop.add_signal_handler(signal.SIGINT, _on_signal, signal.SIGINT)
        loop.add_signal_handler(signal.SIGTERM, _on_signal, signal.SIGTERM)
        logger.info("Signal handlers registered")
    except NotImplementedError:
        logger.debug("Signal handlers not supported on this platform")

async def on_startup_app(app):
    global _updates_queue, _worker_task
    logger.info("on_startup_app: initializing")

    # optional per-app init
    init_fn = getattr(bot_app, "init_app_for_runtime", None)
    if init_fn:
        try:
            if asyncio.iscoroutinefunction(init_fn):
                await init_fn(app)
            else:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, init_fn, app)
            logger.info("bot_app.init_app_for_runtime completed")
        except Exception:
            logger.exception("bot_app.init_app_for_runtime raised exception (continuing startup)")

    _updates_queue = asyncio.Queue(maxsize=2000)
    app['updates_queue'] = _updates_queue
    _worker_task = asyncio.create_task(webhook_worker())

    try:
        sess = await bot.get_session()
        app['bot_session'] = sess
        logger.info("Bot session ready")
    except Exception:
        logger.exception("Failed to get bot session on startup (non-fatal)")

    if KEEP_BOT_ALIVE:
        app['keep_alive_ping'] = asyncio.create_task(keep_alive_ping())
        logger.info("keep_alive_ping started (KEEP_BOT_ALIVE=True)")

    app['keep_alive'] = asyncio.create_task(asyncio.sleep(3600 * 24))

    # choose mode
    # if conflict previously detected, skip polling to avoid immediate repeated Terminateds
    if _polling_conflict_detected:
        logger.warning("Polling previously conflicted with another client; skipping polling startup. Consider using webhook.")
        return

    if FORCE_POLLING or KEEP_BOT_ALIVE or not WEBHOOK_URL:
        try:
            ok = await ensure_no_webhook_before_polling()
            if not ok:
                logger.warning("Webhook may still be present remotely; TerminatedByOtherGetUpdates may occur.")
        except Exception:
            logger.exception("ensure_no_webhook_before_polling failed (ignored)")

        if app.get('polling_task') and not app['polling_task'].done():
            logger.info("Polling task already exists; skipping creating a new one.")
        else:
            app['polling_task'] = asyncio.create_task(polling_runner(app))
            logger.info("Polling task started (background).")
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
            if app.get('polling_task') and not app['polling_task'].done():
                logger.info("Polling task already exists; skipping creating a new one.")
            else:
                app['polling_task'] = asyncio.create_task(polling_runner(app))
                logger.info("Polling task started (background).")

async def on_cleanup_app(app):
    global _updates_queue, _worker_task
    logger.info("on_cleanup: starting cleanup (initiator=%s)", _shutdown_initiator.get("signal"))

    if app.get('keep_alive'):
        app['keep_alive'].cancel()
        try:
            await app['keep_alive']
        except Exception:
            pass

    if app.get('keep_alive_ping'):
        app['keep_alive_ping'].cancel()
        try:
            await app['keep_alive_ping']
        except Exception:
            pass

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

    # Best-effort delete webhook (safe)
    try:
        await bot.delete_webhook()
        logger.info("Webhook deletion requested during cleanup (best-effort).")
    except Exception:
        logger.debug("delete_webhook during cleanup failed (ignored)", exc_info=True)

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

    # robustly close bot sessions and client sessions to avoid "Unclosed client session"
    try:
        # close session stored in app as well
        app_sess = app.get('bot_session')
        if app_sess:
            try:
                if hasattr(app_sess, "closed"):
                    if not app_sess.closed:
                        await app_sess.close()
                        logger.info("Closed app['bot_session']")
                else:
                    res = app_sess.close()
                    if asyncio.iscoroutine(res):
                        await res
                        logger.info("Closed app['bot_session'] (coroutine close)")
            except Exception:
                logger.exception("Failed to close app['bot_session']")
    except Exception:
        logger.exception("Error while closing app-stored bot session (ignored)")

    try:
        await _close_bot_sessions()
    except Exception:
        logger.exception("Error while closing bot sessions (ignored)")

    try:
        await bot.close()
        logger.info('Bot closed (client session closed)')
    except Exception:
        logger.exception('Error while closing bot')

    try:
        if conn:
            conn.close()
            logger.info('DB connection closed')
    except Exception:
        logger.debug('DB close failed', exc_info=True)

    logger.info("Cleanup complete")

# admin endpoints
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
        'polling_conflict_detected': _polling_conflict_detected,
    }
    try:
        wh = await bot.get_webhook_info()
        info['telegram_webhook'] = wh.to_python() if wh else None
    except Exception as e:
        info['telegram_webhook_error'] = str(e)
    return web.json_response(info)

async def admin_switch_to_webhook(request: web.Request):
    if not WEBHOOK_URL:
        return web.json_response({"ok": False, "error": "WEBHOOK_URL not configured"}, status=400)
    webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
    try:
        await bot.set_webhook(webhook)
        return web.json_response({"ok": True, "webhook": webhook})
    except Exception as e:
        logger.exception("admin_switch_to_webhook failed")
        return web.json_response({"ok": False, "error": str(e)})

def create_app():
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_post('/webhook', handle_webhook)
    app.router.add_post('/set_webhook', set_webhook_handler)
    app.router.add_get('/debug', debug_handler)
    app.router.add_post('/admin/switch_to_webhook', admin_switch_to_webhook)
    app.on_startup.append(on_startup_app)
    app.on_cleanup.append(on_cleanup_app)
    return app

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    _register_signal_handlers(loop)
    app = create_app()
    logger.info("Starting web app on 0.0.0.0:%s (FORCE_POLLING=%s, KEEP_BOT_ALIVE=%s)", PORT, FORCE_POLLING, KEEP_BOT_ALIVE)
    web.run_app(app, host='0.0.0.0', port=PORT)
