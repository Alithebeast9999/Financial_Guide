#!/usr/bin/env python3
"""
Resilient Telegram Finance Assistant — webhook queue + optional resilient polling.
Key env flags:
 - KEEP_BOT_ALIVE=1  -> при cleanup НЕ закрываем bot.session / bot.close() (поведение "не закрывать никогда")
 - FORCE_POLLING=1   -> форсировать polling (не ставить webhook)
"""
import os
import logging
import sqlite3
import asyncio
from datetime import datetime, timedelta
import pytz
from aiohttp import web

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import Update as TgUpdate
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# ========== Config ===========
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # e.g. https://financial-guide.onrender.com
PORT = int(os.environ.get("PORT", 10000))
TZ = pytz.timezone("Europe/Moscow")
FORCE_POLLING = os.environ.get("FORCE_POLLING", "0") == "1"
KEEP_BOT_ALIVE = os.environ.get("KEEP_BOT_ALIVE", "0") == "1"  # NEW: если true -> не закрываем бот в cleanup

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

# ========== Bot/Dispatcher =========
bot = Bot(token=BOT_TOKEN, timeout=30, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ========== DB ============
DB_FILE = "bot.db"
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.execute("""CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, income REAL DEFAULT 0, notifications BOOLEAN DEFAULT 1)""")
cursor.execute("""CREATE TABLE IF NOT EXISTS expenses (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, timestamp DATETIME, recurring_id INTEGER DEFAULT NULL)""")
cursor.execute("""CREATE TABLE IF NOT EXISTS recurring (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, day INTEGER)""")
conn.commit()

# ========== (Handlers/logic omitted for brevity) ==========
# For brevity assume your handlers (start, expense handling etc.) are exactly as before.
# Keep same handlers placed here (copy from your working file). 
# -------------------------------------------------------------------------
# (Put all dp.message_handler, dp.callback_query_handler etc. functions here.)
# -------------------------------------------------------------------------

# ========== WEBHOOK QUEUE & WORKER ==========
_updates_queue = None
_worker_task = None

async def webhook_worker():
    logger.info("Webhook worker started")
    try:
        Bot.set_current(bot)
    except Exception:
        logger.debug("Bot.set_current failed in worker")
    while True:
        try:
            update = await _updates_queue.get()
            try:
                try:
                    Bot.set_current(bot)
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
        update = TgUpdate.to_object(data)
    except Exception:
        return web.Response(status=400, text="invalid")
    try:
        _updates_queue.put_nowait(update)
    except asyncio.QueueFull:
        logger.warning("queue full - drop update")
    return web.Response(text="OK")

async def handle_root(request: web.Request):
    return web.Response(text="OK")

# ========== Resilient polling runner (background) ==========
async def polling_runner(app):
    backoff = 1
    max_backoff = 60
    logger.info("Polling runner started")
    while True:
        try:
            try:
                Bot.set_current(bot)
            except Exception:
                pass
            logger.info("Starting dp.start_polling()")
            await dp.start_polling()
            logger.info("dp.start_polling() ended normally")
            break
        except asyncio.CancelledError:
            logger.info("Polling runner cancelled")
            raise
        except Exception:
            logger.exception("Polling crashed, will retry")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)

# ========== Startup / Cleanup ==========
scheduler = AsyncIOScheduler(timezone=TZ)
# (add your scheduler jobs if any)

async def on_startup_app(app):
    global _updates_queue, _worker_task
    logger.info("on_startup_app: initializing")
    _updates_queue = asyncio.Queue(maxsize=2000)
    _worker_task = asyncio.create_task(webhook_worker())
    app['keep_alive'] = asyncio.create_task(asyncio.sleep(3600*24))  # dummy to keep loop busy (or use your keep-alive task)
    scheduler.start()
    logger.info("Scheduler started")

    # If we want persistent bot that should *not* be closed, prefer polling.
    if FORCE_POLLING or KEEP_BOT_ALIVE or not WEBHOOK_URL:
        # Start resilient polling
        app['polling_task'] = asyncio.create_task(polling_runner(app))
        logger.info("Polling mode enabled (background)")
    else:
        # Attempt to set webhook normally
        try:
            await bot.set_webhook(WEBHOOK_URL.rstrip("/") + "/webhook")
            logger.info("Webhook set")
        except Exception:
            logger.exception("set_webhook failed - falling back to polling")
            app['polling_task'] = asyncio.create_task(polling_runner(app))

async def on_cleanup_app(app):
    global _updates_queue, _worker_task
    logger.info("on_cleanup: graceful shutdown")
    # cancel keep-alive
    if app.get('keep_alive'):
        app['keep_alive'].cancel()
        try: await app['keep_alive']
        except: pass
    # cancel worker
    if _worker_task:
        _worker_task.cancel()
        try: await _worker_task
        except: pass
    # drain queue shortly
    if _updates_queue:
        try: await asyncio.wait_for(_updates_queue.join(), timeout=2.0)
        except: pass
    _updates_queue = None
    # stop polling if active
    polling_task = app.get('polling_task')
    if polling_task:
        try: await dp.stop_polling()
        except Exception:
            logger.debug("dp.stop_polling() failed or not available")
        polling_task.cancel()
        try: await polling_task
        except: pass

    # IMPORTANT: do not delete webhook / close bot if KEEP_BOT_ALIVE is True
    if not KEEP_BOT_ALIVE:
        try:
            await bot.delete_webhook()
            logger.info("Webhook deleted on cleanup")
        except Exception:
            logger.debug("Failed to delete webhook on cleanup")

    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        await dp.storage.close(); await dp.storage.wait_closed()
    except Exception:
        pass

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
                    pass
        except Exception:
            logger.exception('Error while closing bot session')
        try:
            await bot.close()
            logger.info('Bot closed (client session closed)')
        except Exception:
            logger.exception('Error while closing bot')
    else:
        logger.info("KEEP_BOT_ALIVE is set -> skipping bot.close() and webhook deletion on cleanup")

    try:
        conn.close()
    except:
        pass
    logger.info("Cleanup complete")

# ========== Debug endpoint ==========
async def debug_handler(request: web.Request):
    info = {
        'queue_size': _updates_queue.qsize() if _updates_queue else None,
        'worker_running': _worker_task is not None and not _worker_task.done(),
        'scheduler_running': scheduler.running,
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

# ========== App creation ==========
def create_app():
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_post('/webhook', handle_webhook)
    app.router.add_get('/debug', debug_handler)
    app.on_startup.append(on_startup_app)
    app.on_cleanup.append(on_cleanup_app)
    return app

if __name__ == '__main__':
    app = create_app()
    logger.info(f"Starting web app on 0.0.0.0:{PORT} (FORCE_POLLING={FORCE_POLLING}, KEEP_BOT_ALIVE={KEEP_BOT_ALIVE})")
    web.run_app(app, host='0.0.0.0', port=PORT)
