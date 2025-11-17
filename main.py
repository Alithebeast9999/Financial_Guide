# main.py
import os
import logging
import asyncio
import signal
import aiohttp
from aiohttp import web
from aiogram.utils.exceptions import TerminatedByOtherGetUpdates

import bot_app

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PORT = int(os.environ.get("PORT", 10000))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # если используете вебхуки
FORCE_POLLING = os.environ.get("FORCE_POLLING", "0") == "1"
KEEP_BOT_ALIVE = os.environ.get("KEEP_BOT_ALIVE", "1") != "0"

# will be set on startup
_app_session = None
bot = None
dp = None
_updates_queue = None
_worker_task = None

async def webhook_worker():
    logger.info("Webhook worker started")
    while True:
        try:
            update = await _updates_queue.get()
            try:
                await dp.process_update(update)
            except Exception:
                logger.exception("Error processing update")
            finally:
                _updates_queue.task_done()
        except asyncio.CancelledError:
            logger.info("Webhook worker cancelled")
            break
        except Exception:
            logger.exception("Unexpected in webhook worker")

async def handle_webhook(request):
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400, text="bad json")
    try:
        update = bot.types.Update(**data)
    except Exception:
        # fallback to raw put into dispatcher
        update = data
    try:
        _updates_queue.put_nowait(update)
    except asyncio.QueueFull:
        logger.warning("Updates queue full — dropping")
    return web.Response(text="ok")

async def ensure_no_webhook(max_retries=6):
    for attempt in range(1, max_retries+1):
        try:
            wh = await bot.get_webhook_info()
            url = getattr(wh, "url", None)
            if not url:
                logger.info("No webhook set (checked attempt %d).", attempt)
                return True
            logger.info("Webhook present: %s (attempt %d) — deleting", url, attempt)
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            logger.exception("ensure_no_webhook attempt %d failed", attempt)
        await asyncio.sleep(min(1 * attempt, 5))
    logger.warning("ensure_no_webhook: max retries reached")
    return False

async def polling_runner(app):
    backoff = 1
    while True:
        try:
            logger.info("Starting dp.start_polling()")
            await ensure_no_webhook()
            await dp.start_polling(timeout=20)
            logger.info("dp.start_polling() ended")
            break
        except TerminatedByOtherGetUpdates:
            logger.warning("TerminatedByOtherGetUpdates detected — deleting webhook and sleeping")
            try:
                await bot.delete_webhook(drop_pending_updates=True)
            except Exception:
                logger.debug("delete_webhook failed during recovery", exc_info=True)
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            logger.info("Polling runner cancelled")
            raise
        except Exception:
            logger.exception("Polling crashed — retrying with backoff")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def keep_alive_ping():
    try:
        while True:
            try:
                await bot.get_me()
            except Exception:
                logger.debug("keep_alive ping failed", exc_info=True)
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        logger.info("keep_alive_ping cancelled")
        raise

async def on_startup_app(app):
    global _app_session, bot, dp, _updates_queue, _worker_task

    logger.info("on_startup_app: creating shared aiohttp session")
    # create a single shared aiohttp session (you control lifecycle)
    _app_session = aiohttp.ClientSession()
    app['shared_session'] = _app_session

    # initialize bot_app with this session
    bot_obj, dp_obj = bot_app.init_bot(_app_session)
    bot = bot_obj
    dp = dp_obj

    # queue + worker for webhook
    _updates_queue = asyncio.Queue(maxsize=2000)
    app['updates_queue'] = _updates_queue
    _worker_task = asyncio.create_task(webhook_worker())

    # keepalives
    if KEEP_BOT_ALIVE:
        app['keep_alive_ping'] = asyncio.create_task(keep_alive_ping())

    # start polling or webhook
    if FORCE_POLLING or not WEBHOOK_URL:
        app['polling_task'] = asyncio.create_task(polling_runner(app))
    else:
        # try set webhook; fallback to polling
        webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
        try:
            await bot.set_webhook(webhook)
            logger.info("Webhook set: %s", webhook)
        except Exception:
            logger.exception("set_webhook failed — fallback to polling")
            app['polling_task'] = asyncio.create_task(polling_runner(app))

async def on_cleanup_app(app):
    global _app_session, bot, dp, _updates_queue, _worker_task
    logger.info("on_cleanup: starting cleanup")

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

    # stop dispatcher polling if running
    try:
        await dp.stop_polling()
    except Exception:
        logger.debug("dp.stop_polling failed", exc_info=True)

    if app.get('polling_task'):
        app['polling_task'].cancel()
        try:
            await app['polling_task']
        except Exception:
            pass

    # best-effort delete webhook
    try:
        await bot.delete_webhook()
    except Exception:
        logger.debug("delete_webhook on cleanup failed", exc_info=True)

    # close dispatcher storage
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
    except Exception:
        logger.debug("storage close failed", exc_info=True)

    # close scheduler if any
    try:
        bot_app.scheduler.shutdown(wait=False)
    except Exception:
        logger.debug("scheduler shutdown failed", exc_info=True)

    # close bot via bot.close()
    try:
        await bot.close()
    except Exception:
        logger.exception("bot.close failed")

    # **very important** — close shared aiohttp session we created
    try:
        if _app_session:
            await _app_session.close()
            logger.info("Shared aiohttp session closed")
    except Exception:
        logger.exception("Failed to close shared aiohttp session")

    logger.info("Cleanup complete")

async def handle_root(request):
    return web.Response(text="OK")

def create_app():
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_post('/webhook', handle_webhook)
    app.on_startup.append(on_startup_app)
    app.on_cleanup.append(on_cleanup_app)
    return app

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    # register signals (best-effort)
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: logger.info("SIGINT received"))
        loop.add_signal_handler(signal.SIGTERM, lambda: logger.info("SIGTERM received"))
    except Exception:
        pass

    app = create_app()
    logger.info(f"Starting web app on 0.0.0.0:{PORT}")
    web.run_app(app, host='0.0.0.0', port=PORT)
