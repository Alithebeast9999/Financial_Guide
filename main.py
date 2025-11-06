#!/usr/bin/env python3
# Полный, устойчивый main.py с aiohttp webhook handler и health endpoint.

import os
import logging
import sqlite3
from datetime import datetime, timedelta
import pytz
import asyncio
from aiohttp import web

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import Update as TgUpdate
# (остальные imports — inline buttons и т.д.)
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# === LOGGING ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === CONFIG ===
BOT_TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # https://financial-guide.onrender.com
PORT = int(os.environ.get("PORT", 10000))
TZ = pytz.timezone("Europe/Moscow")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не установлен!")

# === Bot, Dispatcher, Storage ===
bot = Bot(token=BOT_TOKEN, timeout=30, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# === DB === (как у тебя было)
DB_FILE = "bot.db"
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
# ... (создание таблиц users, expenses, recurring — как в твоей версии)
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    income REAL DEFAULT 0,
    notifications BOOLEAN DEFAULT 1
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS expenses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    category TEXT,
    timestamp DATETIME,
    recurring_id INTEGER DEFAULT NULL
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS recurring (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    category TEXT,
    day INTEGER
)
""")
conn.commit()

# === Categories, FSM, utils и т.д. ===
CATEGORIES = {
    'НАДО': {
        'Аренда жилья': 0.35,
        'Продуктовая корзина': 0.15,
        'Комм. услуги': 0.05,
        'Связь': 0.03,
        'Транспорт': 0.05,
        'Личный уход': 0.02,
        'Медицина': 0.08
    },
    'МОГУ': {
        'Инвестиции': 0.05,
        'Подушка безопасности': 0.05
    },
    'ХОЧУ': {
        'Развлечения': 0.07,
        'Отдых - путешествия': 0.05,
        'Покупки': 0.05
    }
}
ALL_CATEGORIES = [c for g in CATEGORIES.values() for c in g]

class IncomeState(StatesGroup): income = State()
class ExpenseState(StatesGroup): amount = State(); category = State()
class RecurringState(StatesGroup): amount = State(); category = State(); day = State()

db_lock = asyncio.Lock()

async def ensure_user(user_id):
    async with db_lock:
        cursor.execute('INSERT OR IGNORE INTO users (user_id) VALUES (?)', (user_id,))
        conn.commit()

def get_income(user_id):
    cursor.execute('SELECT income FROM users WHERE user_id = ?', (user_id,))
    r = cursor.fetchone()
    return float(r['income']) if r and r['income'] is not None else 0.0

def set_income(user_id, income):
    cursor.execute('INSERT OR REPLACE INTO users (user_id, income) VALUES (?, ?)', (user_id, income))
    conn.commit()

def format_amount(amount: float) -> str:
    try:
        return f"{amount:,.0f}".replace(",", " ")
    except:
        return str(amount)

def get_limits_from_income(income: float) -> dict:
    return {cat: income * pct for group in CATEGORIES.values() for cat, pct in group.items()}

def get_limits(user_id: int) -> dict:
    income = get_income(user_id)
    return get_limits_from_income(income)

def add_expense(user_id, amount, category, ts=None, rec_id=None):
    ts = ts or datetime.now(TZ)
    cursor.execute('INSERT INTO expenses (user_id, amount, category, timestamp, recurring_id) VALUES (?, ?, ?, ?, ?)',
                   (user_id, amount, category, ts.isoformat(), rec_id))
    conn.commit()

def get_expenses(user_id, limit=10):
    cursor.execute('SELECT id, amount, category, timestamp FROM expenses WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?', (user_id, limit))
    return cursor.fetchall()

def delete_expense(exp_id):
    cursor.execute('DELETE FROM expenses WHERE id = ?', (exp_id,))
    conn.commit()

def check_limits(user_id, category, amount):
    limits = get_limits(user_id)
    if category not in limits: return []
    income = get_income(user_id)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    cursor.execute('SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ?', (user_id, month_start.isoformat(), month_end.isoformat()))
    total_spent = cursor.fetchone()['total'] or 0
    cursor.execute('SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND category = ? AND timestamp BETWEEN ? AND ?', (user_id, category, month_start.isoformat(), month_end.isoformat()))
    cat_spent = cursor.fetchone()['total'] or 0
    msgs = []
    if total_spent + amount > income:
        msgs.append("⚠️ Общий месячный лимит превышен!")
    if cat_spent + amount > limits[category]:
        msgs.append(f"⚠️ Лимит по '{category}' превышен!")
    elif cat_spent + amount > 0.9 * limits[category]:
        msgs.append(f"⚠️ Ты израсходовал более 90% лимита по '{category}'!")
    return msgs

# === Scheduler ===
scheduler = AsyncIOScheduler(timezone=TZ)
async def daily_reminders(): 
    cursor.execute('SELECT user_id FROM users WHERE notifications = 1')
    for (uid,) in cursor.fetchall():
        try: await bot.send_message(uid, "💡 Не забудь добавить траты за сегодня!")
        except Exception as e: logger.debug(e)
async def weekly_report():
    cursor.execute('SELECT user_id FROM users')
    for (uid,) in cursor.fetchall():
        try: await bot.send_message(uid, "📊 Еженедельный отчёт:\n\n" + format_stats(uid))
        except Exception as e: logger.debug(e)
async def process_recurring():
    today = datetime.now(TZ).day
    cursor.execute('SELECT id, user_id, amount, category FROM recurring WHERE day = ?', (today,))
    for r in cursor.fetchall():
        rec_id, uid, amt, cat = r
        add_expense(uid, amt, cat, rec_id=rec_id)
        try: await bot.send_message(uid, f"🔁 Добавлен регулярный расход: {amt:,.0f} ₽ — {cat}")
        except Exception as e: logger.debug(e)

scheduler.add_job(daily_reminders, CronTrigger(hour=9, minute=0))
scheduler.add_job(weekly_report, CronTrigger(day_of_week='mon', hour=9, minute=0))
scheduler.add_job(process_recurring, CronTrigger(hour=6, minute=0))

# === Handlers (start / income / expenses / history / etc.) ===
# (скопируй сюда все обработчики из твоей текущей версии — они останутся прежними)
# Для краткости в этом фрагменте я предполагаю, что они уже есть в файле.

# === WEBHOOK / AIOHTTP APP ===

# === NEW: Safe webhook handling with queue + background worker ===
import asyncio
from aiogram.types import Update as TgUpdate

# global queue for incoming updates
_updates_queue: "asyncio.Queue[TgUpdate]" = None
_worker_task: asyncio.Task = None

async def webhook_worker():
    """Background worker: берёт апдейты из очереди и обрабатывает через dp.process_update."""
    logger.info("Webhook worker started")
    while True:
        try:
            update = await _updates_queue.get()
            try:
                # process update inside try/except so exceptions won't kill the worker
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
    """AIOHTTP POST handler for Telegram webhook — push update to queue and return 200 quickly."""
    try:
        data = await request.json()
    except Exception:
        logger.exception("Webhook: invalid JSON")
        return web.Response(status=400, text="invalid json")

    try:
        update = TgUpdate.to_object(data)
    except Exception:
        logger.exception("Webhook: can't parse update to aiogram Update")
        return web.Response(status=400, text="invalid update")

    # put update into queue (do not await processing here)
    try:
        _updates_queue.put_nowait(update)
    except asyncio.QueueFull:
        # queue full -> log and still return 200 to avoid Telegram retry storm
        logger.warning("Updates queue is full — dropping update")
    except Exception:
        logger.exception("Failed to enqueue update")

    # quick response to Telegram
    return web.Response(status=200, text="OK")

async def handle_root(request: web.Request):
    return web.Response(text="OK")

async def on_startup_app(app: web.Application):
    global _updates_queue, _worker_task
    # prepare queue and start worker
    _updates_queue = asyncio.Queue(maxsize=1000)
    _worker_task = asyncio.create_task(webhook_worker())

    # start scheduler
    try:
        scheduler.start()
        logger.info("Scheduler started")
    except Exception:
        logger.exception("Failed to start scheduler")

    # set webhook URL at Telegram (best effort)
    if WEBHOOK_URL:
        webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
        try:
            await bot.set_webhook(webhook)
            logger.info(f"Webhook установлен: {webhook}")
        except Exception:
            logger.exception("Не удалось установить webhook (on_startup)")

async def on_cleanup_app(app: web.Application):
    global _updates_queue, _worker_task
    # cancel worker gracefully
    if _worker_task:
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            logger.info("Worker cancelled successfully")
        except Exception:
            logger.exception("Exception when cancelling worker")

    # ensure queue drained (optional) - not waiting here to speed up shutdown
    _updates_queue = None

    # delete webhook
    try:
        await bot.delete_webhook()
        logger.info("Webhook deleted on cleanup")
    except Exception:
        logger.debug("Could not delete webhook on cleanup")

    # shutdown scheduler
    try:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shutdown")
    except Exception:
        logger.debug("Scheduler shutdown failed")

    # close FSM storage
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
    except Exception:
        logger.debug("Storage close failed")

    logger.info("Cleanup complete")

def create_app():
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_post('/webhook', handle_webhook)
    app.on_startup.append(on_startup_app)
    app.on_cleanup.append(on_cleanup_app)
    return app

# Run app (at bottom of file)
if __name__ == '__main__':
    app = create_app()
    web.run_app(app, host='0.0.0.0', port=PORT)
