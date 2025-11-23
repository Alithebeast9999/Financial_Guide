# main.py
import os
import logging
import asyncio
import signal
from aiohttp import web

from aiogram.utils.exceptions import TerminatedByOtherGetUpdates
from aiogram.types import Update

import bot_app  # imports bot, dp, scheduler, handlers are registered on import

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
_updates_queue: asyncio.Queue | None = None
_worker_task: asyncio.Task | None = None

_shutdown_initiator = {"signal": None}


async def webhook_worker():
    """Worker that takes Update objects from the queue and processes them via Dispatcher."""
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
    """Handler for Telegram webhook POSTs. Enqueues Update for processing."""
    try:
        data = await request.json()
        # Create aiogram Update object from dict. aiogram Update will accept kwargs for fields.
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


async def metrics_handler(request: web.Request):
    """
    Simple JSON metrics: users_count, expenses_count, pending_count.
    Uses bot_app.db_fetchone safely.
    """
    try:
        try:
            r = await bot_app.db_fetchone("SELECT COUNT(*) as c FROM users", ())
            users_count = int(r["c"]) if r else 0
        except Exception:
            users_count = 0
        try:
            r = await bot_app.db_fetchone("SELECT COUNT(*) as c FROM expenses", ())
            expenses_count = int(r["c"]) if r else 0
        except Exception:
            expenses_count = 0
        try:
            r = await bot_app.db_fetchone("SELECT COUNT(*) as c FROM pending", ())
            pending_count = int(r["c"]) if r else 0
        except Exception:
            pending_count = 0
        payload = {
            "users_count": users_count,
            "expenses_count": expenses_count,
            "pending_count": pending_count,
            "webhook_url": bool(WEBHOOK_URL),
            "keep_bot_alive": KEEP_BOT_ALIVE,
        }
        return web.json_response(payload)
    except Exception:
        logger.exception("metrics_handler failed")
        return web.json_response({"ok": False}, status=500)


async def debug_handler(request: web.Request):
    """Admin debug endpoint — queue sizes, worker status, webhook info."""
    info = {
        'queue_size': _updates_queue.qsize() if _updates_queue else None,
        'worker_running': _worker_task is not None and not _worker_task.done(),
        'scheduler_running': getattr(scheduler, "running", None),
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


_shutdown_lock = asyncio.Lock()


def _register_signal_handlers(loop: asyncio.AbstractEventLoop):
    def _on_signal(sig):
        _shutdown_initiator["signal"] = str(sig)
        logger.info(f"Process received signal: {sig}. Marking shutdown_initiator.")
    try:
        loop.add_signal_handler(signal.SIGINT, _on_signal, signal.SIGINT)
        loop.add_signal_handler(signal.SIGTERM, _on_signal, signal.SIGTERM)
        logger.info("Signal handlers registered")
    except NotImplementedError:
        logger.debug("Signal handlers not supported on this platform")


async def keep_alive_ping():
    """Periodically call bot.get_me() to keep underlying session alive (if desired)."""
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


async def polling_runner(app):
    """
    Start polling loop — used if webhook setup fails or FORCE_POLLING is True.
    Robust restart/backoff logic included.
    """
    backoff = 1
    max_backoff = 60
    logger.info("Polling runner started")
    while True:
        try:
            logger.info("Starting dp.start_polling()")
            await dp.start_polling(timeout=20)
            logger.info("dp.start_polling() ended normally")
            break
        except asyncio.CancelledError:
            logger.info("Polling runner cancelled")
            raise
        except TerminatedByOtherGetUpdates:
            logger.warning("TerminatedByOtherGetUpdates — another getUpdates exists. Sleeping 60s.")
            await asyncio.sleep(60)
            backoff = 1
        except Exception:
            logger.exception("Polling crashed, will retry")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)


async def on_startup_app(app: web.Application):
    """
    Startup sequence:
    - initialize bot_app (DB, scheduler, pending restore)
    - start updates queue and worker
    - create/get bot session
    - optionally start keep-alive ping
    - choose between webhook or polling
    """
    global _updates_queue, _worker_task
    logger.info("on_startup_app: initializing")

    # Initialize bot_app internals (db_lock, scheduler jobs, pending actions, etc.)
    await bot_app.init_app_for_runtime(app)

    # Queue + worker for webhook handling
    _updates_queue = asyncio.Queue(maxsize=2000)
    app['updates_queue'] = _updates_queue
    _worker_task = asyncio.create_task(webhook_worker())

    # obtain bot session inside async context to avoid DeprecationWarning
    try:
        sess = await bot.get_session()
        app['bot_session'] = sess
        logger.info("Bot session ready")
    except Exception:
        logger.exception("Failed to get bot session on startup (non-fatal)")

    # start keep-alive ping if requested
    if KEEP_BOT_ALIVE:
        app['keep_alive_ping'] = asyncio.create_task(keep_alive_ping())
        logger.info("keep_alive_ping started (KEEP_BOT_ALIVE=True)")

    # small dummy to keep loop busy (optional)
    app['keep_alive'] = asyncio.create_task(asyncio.sleep(3600 * 24))

    # choose mode: webhook if WEBHOOK_URL provided and not forcing polling
    if FORCE_POLLING or KEEP_BOT_ALIVE or not WEBHOOK_URL:
        # ensure no webhook blocking polling
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook deleted (pre-polling cleanup)")
        except Exception:
            logger.debug("delete_webhook pre-polling failed (ignored)")
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


async def on_cleanup_app(app: web.Application):
    """
    Cleanup procedure on shutdown:
    - cancel keep_alive tasks
    - cancel worker and drain queue shortly
    - stop polling if any
    - delete webhook (depending on KEEP_BOT_ALIVE)
    - shutdown scheduler
    - close dp.storage
    - close bot session and bot
    - close DB via bot_app.close_db()
    """
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

    # delete webhook only if NOT KEEP_BOT_ALIVE
    if not KEEP_BOT_ALIVE:
        try:
            await bot.delete_webhook()
            logger.info("Webhook deleted on cleanup")
        except Exception:
            logger.debug("Failed to delete webhook on cleanup", exc_info=True)
    else:
        logger.info("KEEP_BOT_ALIVE=True -> skipping webhook deletion")

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
            logger.info('Bot closed (client session closed)')
        except Exception:
            logger.exception('Error while closing bot')
    else:
        logger.info('KEEP_BOT_ALIVE=True -> skipping bot.close()')

    # close aiosqlite connection via bot_app
    try:
        await bot_app.close_db()
    except Exception:
        logger.exception('aiosqlite close failed', exc_info=True)

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


def create_app():
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_post('/webhook', handle_webhook)
    app.router.add_get('/healthz', handle_root)
    app.router.add_post('/set_webhook', set_webhook_handler)
    app.router.add_get('/debug', debug_handler)
    app.router.add_get('/metrics', metrics_handler)
    # startup / cleanup hooks
    app.on_startup.append(on_startup_app)
    app.on_cleanup.append(on_cleanup_app)
    return app


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    _register_signal_handlers(loop)
    app = create_app()
    logger.info(f"Starting web app on 0.0.0.0:{PORT} (FORCE_POLLING={FORCE_POLLING}, KEEP_BOT_ALIVE={KEEP_BOT_ALIVE})")
    web.run_app(app, host='0.0.0.0', port=PORT)
