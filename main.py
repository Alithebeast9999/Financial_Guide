# main.py (webhook-only, optimized for Render) — обновлённый фрагмент с close_db
import os
import logging
import asyncio
import signal
from aiohttp import web

import bot_app  # imports bot, dp, scheduler, handlers are registered on import
from aiogram.types import Update

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # Render usually generates this
PORT = int(os.environ.get("PORT", 10000))
# This main.py is webhook-only by design. If WEBHOOK_URL is not provided,
# the app will start but won't set webhook automatically.

# Shared references from bot_app
bot = bot_app.bot
dp = bot_app.dp
scheduler = bot_app.scheduler

# Runtime artifacts
_updates_queue: asyncio.Queue | None = None
_worker_task: asyncio.Task | None = None

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


async def webhook_worker():
    """Background worker that processes Update objects from the queue."""
    logger.info("Webhook worker started")
    # try to set Bot as current for aiogram internals (best-effort)
    try:
        bot_app.Bot.set_current(bot)
    except Exception:
        logger.debug("Bot.set_current failed in worker")
    while True:
        try:
            update: Update = await _updates_queue.get()
            try:
                # ensure current bot context for handlers
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
    Webhook endpoint: receives Telegram POST, builds aiogram.types.Update,
    enqueues for background processing and returns HTTP 200 quickly.
    """
    try:
        data = await request.json()
        update = Update(**data)
    except Exception:
        logger.exception("handle_webhook: invalid payload")
        # respond 400 to indicate malformed payload
        return web.Response(status=400, text="invalid")
    # enqueue quickly; if full, drop update (log) but still return 200 to Telegram
    try:
        _updates_queue.put_nowait(update)
    except asyncio.QueueFull:
        logger.warning("updates queue full - dropping incoming update")
    return web.Response(text="OK")


async def handle_root(request: web.Request):
    return web.Response(text="OK")


async def health_handler(request: web.Request):
    return web.Response(text="OK")


async def set_webhook_handler(request: web.Request):
    """
    Admin endpoint to (re)install webhook based on WEBHOOK_URL env var.
    Useful for manual / on-demand webhook setup.
    """
    if not WEBHOOK_URL:
        return web.json_response({"ok": False, "error": "WEBHOOK_URL not configured"}, status=400)
    webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
    try:
        await bot.set_webhook(webhook)
        logger.info("Webhook set via admin endpoint: %s", webhook)
        return web.json_response({"ok": True, "webhook": webhook})
    except Exception as e:
        logger.exception("set_webhook_handler failed")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def debug_handler(request: web.Request):
    info = {
        'queue_size': _updates_queue.qsize() if _updates_queue else None,
        'worker_running': _worker_task is not None and not _worker_task.done(),
        'scheduler_running': getattr(scheduler, "running", None),
        'webhook_url_env': WEBHOOK_URL,
    }
    try:
        wh = await bot.get_webhook_info()
        info['telegram_webhook'] = wh.to_python() if wh else None
    except Exception as e:
        info['telegram_webhook_error'] = str(e)
    return web.json_response(info)


async def on_startup_app(app: web.Application):
    """Initialize bot_app internals, queue and worker, and set webhook (if provided)."""
    global _updates_queue, _worker_task
    logger.info("on_startup_app: initializing")

    # initialize bot_app (db_lock, scheduler jobs, etc.)
    await bot_app.init_app_for_runtime(app)

    # create queue + worker for webhook updates
    _updates_queue = asyncio.Queue(maxsize=2000)
    app['updates_queue'] = _updates_queue
    _worker_task = asyncio.create_task(webhook_worker())

    # obtain bot session (best-effort)
    try:
        sess = await bot.get_session()
        app['bot_session'] = sess
        logger.info("Bot session ready")
    except Exception:
        logger.debug("Failed to get bot session on startup (non-fatal)", exc_info=True)

    # set webhook if WEBHOOK_URL configured
    if WEBHOOK_URL:
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
            logger.warning("set_webhook failed after retries - webhook not set. Check WEBHOOK_URL and network.")
    else:
        logger.warning("WEBHOOK_URL not configured; webhook not set. App will still run but won't receive Telegram updates.")


async def on_cleanup_app(app: web.Application):
    """Shutdown tasks, delete webhook, shutdown scheduler and close DB/bot resources."""
    global _updates_queue, _worker_task
    logger.info("on_cleanup: starting cleanup (initiator=%s)", _shutdown_initiator.get("signal"))

    # cancel worker task
    if _worker_task:
        _worker_task.cancel()
        try:
            await _worker_task
        except Exception:
            pass

    # drain queue briefly
    if _updates_queue:
        try:
            await asyncio.wait_for(_updates_queue.join(), timeout=2.0)
        except Exception:
            pass
    _updates_queue = None

    # delete webhook (best-effort)
    if WEBHOOK_URL:
        try:
            await bot.delete_webhook()
            logger.info("Webhook deleted on cleanup")
        except Exception:
            logger.debug("Failed to delete webhook on cleanup", exc_info=True)

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

    # close bot session & bot
    try:
        sess = None
        try:
            sess = await bot.get_session()
        except Exception:
            sess = getattr(bot, 'session', None)
        if sess:
            try:
                await sess.close()
                logger.info("bot.session closed explicitly")
            except Exception:
                logger.exception("Error while closing bot session")
    except Exception:
        logger.exception("Failed while closing bot session")
    try:
        await bot.close()
        logger.info("Bot closed (client session closed)")
    except Exception:
        logger.exception("Error while closing bot")

    # close aiosqlite DB (bot_app.close_db handles guard)
    try:
        await bot_app.close_db()
    except Exception:
        logger.exception("Error while closing DB via bot_app.close_db()")

    logger.info("Cleanup complete")


def create_app():
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_post("/webhook", handle_webhook)
    app.router.add_get("/healthz", health_handler)
    app.router.add_post("/set_webhook", set_webhook_handler)
    app.router.add_get("/debug", debug_handler)
    app.on_startup.append(on_startup_app)
    app.on_cleanup.append(on_cleanup_app)
    return app


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    _register_signal_handlers(loop)
    app = create_app()
    logger.info(f"Starting webhook-only web app on 0.0.0.0:{PORT}")
    web.run_app(app, host="0.0.0.0", port=PORT)
