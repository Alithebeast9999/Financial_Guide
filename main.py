#!/usr/bin/env python3
"""
Resilient Telegram Finance Assistant — webhook queue + optional resilient polling.

Defaults:
 - KEEP_BOT_ALIVE default is ON (so bot won't be closed inside on_cleanup).
 - FORCE_POLLING can be set to "1" to force polling mode.
"""
import os
import logging
import sqlite3
import asyncio
import signal
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

# ---------------- Config ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # e.g. https://financial-guide.onrender.com
PORT = int(os.environ.get("PORT", 10000))
TZ = pytz.timezone("Europe/Moscow")

# NOTE: default KEEP_BOT_ALIVE is True now (so bot won't be closed in on_cleanup)
FORCE_POLLING = os.environ.get("FORCE_POLLING", "0") == "1"
KEEP_BOT_ALIVE = os.environ.get("KEEP_BOT_ALIVE", "1") != "0"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

# ---------------- Bot / Dispatcher ---------------
bot = Bot(token=BOT_TOKEN, timeout=30, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ---------------- DB ----------------------
DB_FILE = "bot.db"
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.execute("""CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, income REAL DEFAULT 0, notifications BOOLEAN DEFAULT 1)""")
cursor.execute("""CREATE TABLE IF NOT EXISTS expenses (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, timestamp DATETIME, recurring_id INTEGER DEFAULT NULL)""")
cursor.execute("""CREATE TABLE IF NOT EXISTS recurring (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, day INTEGER)""")
conn.commit()

# ---------------- Categories & states (copy your full handlers here) -----------------
# --- Paste all your handlers and helper functions below as they were in your working file ---
# For brevity I leave placeholders — in your repo you must include actual handlers (start, add expense, callbacks, etc.)
# -------------------------------------------------------------------------
# Example minimal handlers (if you have full ones, keep them)
class IncomeState(StatesGroup):
    income = State()
class ExpenseState(StatesGroup):
    amount = State()
    category = State()
class RecurringState(StatesGroup):
    amount = State()
    category = State()
    day = State()

@dp.message_handler(commands=['start'])
async def start_handler(msg: types.Message):
    await msg.reply("Bot started — handlers must be restored in the main.py file.")

# -------------------------------------------------------------------------
# Make sure to paste your full handlers here when deploying this file.

# ---------------- WEBHOOK queue & worker ----------------
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
        logger.exception("handle_webhook: invalid payload")
        return web.Response(status=400, text="invalid")
    try:
        _updates_queue.put_nowait(update)
    except asyncio.QueueFull:
        logger.warning("Updates queue is full — dropping update")
    return web.Response(text="OK")

async def handle_root(request: web.Request):
    return web.Response(text="OK")

# ---------------- Resilient polling runner ----------------
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

# ---------------- Scheduler ----------------
scheduler = AsyncIOScheduler(timezone=TZ)
# (add your scheduler jobs if any, like daily_reminders etc.)

# ---------------- Startup / Cleanup ----------------
# track shutdown initiator for better diagnostics
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

async def on_startup_app(app):
    global _updates_queue, _worker_task
    logger.info("on_startup_app: initializing")
    _updates_queue = asyncio.Queue(maxsize=2000)
    _worker_task = asyncio.create_task(webhook_worker())
    # keep-alive dummy task so loop stays 'busy' (optional)
    app['keep_alive'] = asyncio.create_task(asyncio.sleep(3600*24))
    try:
        scheduler.start()
        logger.info("Scheduler started")
    except Exception:
        logger.exception("Failed to start scheduler")

    # select mode: polling preferred when KEEP_BOT_ALIVE or FORCE_POLLING
    if FORCE_POLLING or KEEP_BOT_ALIVE or not WEBHOOK_URL:
        # remove any webhook first to avoid conflicts
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook deleted (pre-polling cleanup)")
        except Exception:
            logger.exception("Failed to delete webhook before polling")
        app['polling_task'] = asyncio.create_task(polling_runner(app))
        logger.info("Polling mode enabled (background)")
    else:
        # attempt set webhook
        try:
            await bot.set_webhook(WEBHOOK_URL.rstrip("/") + "/webhook")
            logger.info("Webhook set successfully")
        except Exception:
            logger.exception("set_webhook failed - falling back to polling")
            try:
                await bot.delete_webhook(drop_pending_updates=True)
                logger.info("Webhook deleted during fallback")
            except Exception:
                logger.exception("Failed to delete webhook during fallback")
            app['polling_task'] = asyncio.create_task(polling_runner(app))

async def on_cleanup_app(app):
    global _updates_queue, _worker_task
    logger.info("on_cleanup: graceful shutdown (initiator=%s)", _shutdown_initiator.get("signal"))
    # cancel keep-alive
    if app.get('keep_alive'):
        app['keep_alive'].cancel()
        try:
            await app['keep_alive']
        except Exception:
            pass
    # cancel worker
    if _worker_task:
        _worker_task.cancel()
        try:
            await _worker_task
        except Exception:
            pass
    # try to drain queue shortly
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
            logger.debug("dp.stop_polling() failed or not available")
        polling_task.cancel()
        try:
            await polling_task
        except Exception:
            pass

    # By design: if KEEP_BOT_ALIVE is True -> skip deleting webhook and skip bot.close()
    if not KEEP_BOT_ALIVE:
        try:
            await bot.delete_webhook()
            logger.info("Webhook deleted on cleanup")
        except Exception:
            logger.debug("Failed to delete webhook on cleanup")

    try:
        scheduler.shutdown(wait=False)
    except Exception:
        logger.debug("Scheduler shutdown failed")

    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
    except Exception:
        logger.debug("Storage close failed")

    # close bot session & bot only if NOT KEEP_BOT_ALIVE
    if not KEEP_BOT_ALIVE:
        try:
            sess = None
            try:
                sess = await bot.get_session()  # new API if available
            except Exception:
