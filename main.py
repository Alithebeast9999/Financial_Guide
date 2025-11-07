#!/usr/bin/env python3
# resilient main.py — polling auto-reconnect + webhook fallback
import os, logging, sqlite3, asyncio
from datetime import datetime, timedelta
import pytz
from aiohttp import web

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import Update as TgUpdate
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# config
BOT_TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 10000))
TZ = pytz.timezone("Europe/Moscow")
# If set to "1" -> force polling mode
FORCE_POLLING = os.environ.get("FORCE_POLLING", "0") == "1"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

# bot/dispatcher/storage
bot = Bot(token=BOT_TOKEN, timeout=30, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# DB (same schema)
DB_FILE = "bot.db"
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.execute("""CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, income REAL DEFAULT 0, notifications BOOLEAN DEFAULT 1)""")
cursor.execute("""CREATE TABLE IF NOT EXISTS expenses (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, timestamp DATETIME, recurring_id INTEGER DEFAULT NULL)""")
cursor.execute("""CREATE TABLE IF NOT EXISTS recurring (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, day INTEGER)""")
conn.commit()

# categories and states (same as you had)
CATEGORIES = {
    "НАДО": {"Аренда жилья":0.35,"Продуктовая корзина":0.15,"Комм. услуги":0.05,"Связь":0.03,"Транспорт":0.05,"Личный уход":0.02,"Медицина":0.08},
    "МОГУ": {"Инвестиции":0.05,"Подушка безопасности":0.05},
    "ХОЧУ": {"Развлечения":0.07,"Отдых - путешествия":0.05,"Покупки":0.05}
}
ALL_CATEGORIES = [c for g in CATEGORIES.values() for c in g]

class IncomeState(StatesGroup): income = State()
class ExpenseState(StatesGroup): amount = State(); category = State()
class RecurringState(StatesGroup): amount = State(); category = State(); day = State()

db_lock = asyncio.Lock()
def format_amount(x): 
    try: return f"{x:,.0f}".replace(",", " ")
    except: return str(x)
def get_limits_from_income(income):
    return {cat: income * pct for group in CATEGORIES.values() for cat,pct in group.items()}

# db helpers (same logic)...
async def ensure_user(uid):
    async with db_lock:
        cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (uid,))
        conn.commit()
def get_income(uid):
    cursor.execute("SELECT income FROM users WHERE user_id = ?", (uid,))
    r = cursor.fetchone()
    return float(r["income"]) if r and r["income"] is not None else 0.0
def set_income(uid, v):
    cursor.execute("INSERT OR REPLACE INTO users (user_id, income) VALUES (?, ?)", (uid, v)); conn.commit()
def add_expense(uid, amount, category, ts=None, rec_id=None):
    ts = ts or datetime.now(TZ)
    cursor.execute("INSERT INTO expenses (user_id, amount, category, timestamp, recurring_id) VALUES (?, ?, ?, ?, ?)",
                   (uid, amount, category, ts.isoformat(), rec_id)); conn.commit()
def get_expenses(uid, limit=10):
    cursor.execute("SELECT id, amount, category, timestamp FROM expenses WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?", (uid, limit))
    return cursor.fetchall()
def delete_expense(eid):
    cursor.execute("DELETE FROM expenses WHERE id = ?", (eid,)); conn.commit()
def check_limits(uid, category, amount):
    limits = get_limits_from_income(get_income(uid))
    if category not in limits: return []
    income = get_income(uid)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    cursor.execute("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ?", (uid, month_start.isoformat(), month_end.isoformat()))
    total_spent = cursor.fetchone()["total"] or 0
    cursor.execute("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND category = ? AND timestamp BETWEEN ? AND ?", (uid, category, month_start.isoformat(), month_end.isoformat()))
    cat_spent = cursor.fetchone()["total"] or 0
    msgs=[]
    if total_spent + amount > income: msgs.append("⚠️ Общий месячный лимит превышен!")
    if cat_spent + amount > limits[category]: msgs.append(f"⚠️ Лимит по '{category}' превышен!")
    elif cat_spent + amount > 0.9 * limits[category]: msgs.append(f"⚠️ Ты израсходовал более 90% лимита по '{category}'!")
    return msgs

# format_stats/build_limits_table_html etc (same as before)...
def format_stats(uid):
    income = get_income(uid); limits = get_limits_from_income(income)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    cursor.execute("SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ? GROUP BY category",
                   (uid, month_start.isoformat(), month_end.isoformat()))
    rows = cursor.fetchall(); spent = {r["category"]: r["total"] for r in rows}
    text = f"💰 Ваш доход: {format_amount(income)} ₽\n\n"
    for group, cats in CATEGORIES.items():
        text += f"📂 {group}\n"
        for cat, pct in cats.items():
            lim = limits.get(cat, 0); s = spent.get(cat, 0) or 0
            perc = (s / lim * 100) if lim else 0
            text += f"• {cat}: {s:,.0f} ₽ / {lim:,.0f} ₽ ({perc:.0f}%)\n"
        text += "\n"
    return text

# Scheduler jobs as before (daily_reminders, weekly_report, process_recurring)
scheduler = AsyncIOScheduler(timezone=TZ)
async def daily_reminders():
    cursor.execute("SELECT user_id FROM users WHERE notifications = 1")
    for (uid,) in cursor.fetchall():
        try: await bot.send_message(uid, "💡 Не забудь добавить траты за сегодня!")
        except Exception as e: logger.debug(e)
async def weekly_report():
    cursor.execute("SELECT user_id FROM users")
    for (uid,) in cursor.fetchall():
        try: await bot.send_message(uid, "📊 Еженедельный отчёт:\n\n" + format_stats(uid))
        except Exception as e: logger.debug(e)
async def process_recurring():
    today = datetime.now(TZ).day
    cursor.execute("SELECT id, user_id, amount, category FROM recurring WHERE day = ?", (today,))
    for r in cursor.fetchall():
        rec_id, uid, amt, cat = r
        add_expense(uid, amt, cat, rec_id=rec_id)
        try: await bot.send_message(uid, f"🔁 Добавлен регулярный расход: {amt:,.0f} ₽ — {cat}")
        except: pass
scheduler.add_job(daily_reminders, CronTrigger(hour=9, minute=0))
scheduler.add_job(weekly_report, CronTrigger(day_of_week='mon', hour=9, minute=0))
scheduler.add_job(process_recurring, CronTrigger(hour=6, minute=0))

# Handlers: start, set_income_handler, add expense etc. (copy your handlers)
# ... (omitted here for brevity; keep your existing handlers exactly)

# WEBHOOK handling queue (same pattern)
_updates_queue = None
_worker_task = None

async def webhook_worker():
    logger.info("Webhook worker started")
    try: Bot.set_current(bot)
    except: pass
    while True:
        try:
            update = await _updates_queue.get()
            try:
                Bot.set_current(bot)
            except: pass
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

async def handle_webhook(request):
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

# keep-alive
async def keep_alive():
    try:
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        return

# robust polling task with reconnect logic
async def polling_runner(app):
    # This runner will keep trying to start polling; if it dies, it will restart with backoff
    backoff = 1
    max_backoff = 60
    while True:
        try:
            Bot.set_current(bot)
        except: pass
        try:
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

# startup / cleanup
async def on_startup_app(app):
    global _updates_queue, _worker_task
    _updates_queue = asyncio.Queue(maxsize=2000)
    _worker_task = asyncio.create_task(webhook_worker())
    app['keep_alive'] = asyncio.create_task(keep_alive())
    scheduler.start()
    # choose mode
    if FORCE_POLLING or not WEBHOOK_URL:
        # start resilient polling in background
        app['polling_task'] = asyncio.create_task(polling_runner(app))
        logger.info("Polling mode enabled")
    else:
        # attempt webhook
        try:
            await bot.set_webhook(WEBHOOK_URL.rstrip("/") + "/webhook")
            logger.info("Webhook set")
        except Exception:
            logger.exception("set_webhook failed - falling back to polling")
            app['polling_task'] = asyncio.create_task(polling_runner(app))

async def on_cleanup_app(app):
    global _updates_queue, _worker_task
    # cancel keep-alive
    if app.get('keep_alive'):
        app['keep_alive'].cancel(); 
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
        except: pass
        polling_task.cancel()
        try: await polling_task
        except: pass
    # remove webhook
    try: await bot.delete_webhook()
    except: pass
    try: scheduler.shutdown(wait=False)
    except: pass
    try:
        await dp.storage.close(); await dp.storage.wait_closed()
    except: pass
    # close bot session & bot
    try:
        sess = getattr(bot, "session", None)
        if sess:
            try: await sess.close()
            except: pass
    except: pass
    try: await bot.close()
    except: pass
    try: conn.close()
    except: pass

def create_app():
    app = web.Application()
    app.router.add_get("/", lambda r: web.Response(text="OK"))
    app.router.add_post("/webhook", handle_webhook)
    app.on_startup.append(on_startup_app)
    app.on_cleanup.append(on_cleanup_app)
    return app

if __name__ == "__main__":
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=PORT)
