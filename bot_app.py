# bot_app.py
"""
Core bot module — contains bot, dp, handlers, scheduler and init_app_for_runtime.

This is your working bot_app with a few safe hardening touches:
 - db_lock is created lazily but guaranteed in init_app_for_runtime
 - scheduler jobs registered idempotently
 - init_app_for_runtime attempts to obtain bot session and populate app['bot_session']
"""
import os
import logging
import sqlite3
import asyncio
from datetime import datetime, timedelta
import pytz

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import Update as TgUpdate

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

TZ = pytz.timezone("Europe/Moscow")

# Bot / Dispatcher
bot = Bot(token=BOT_TOKEN, timeout=30, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# SQLite DB (synchronous, protected by asyncio.Lock)
DB_FILE = "bot.db"
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.execute("""CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, income REAL DEFAULT 0, notifications BOOLEAN DEFAULT 1)""")
cursor.execute("""CREATE TABLE IF NOT EXISTS expenses (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, timestamp DATETIME, recurring_id INTEGER DEFAULT NULL)""")
cursor.execute("""CREATE TABLE IF NOT EXISTS recurring (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, day INTEGER)""")
conn.commit()

# db_lock will be created on app startup (async) in init_app_for_runtime if None
db_lock: Optional[asyncio.Lock] = None

# Categories & FSM states
CATEGORIES = {
    "НАДО": {"Аренда жилья": 0.35, "Продуктовая корзина": 0.15, "Комм. услуги": 0.05, "Связь": 0.03, "Транспорт": 0.05, "Личный уход": 0.02, "Медицина": 0.08},
    "МОГУ": {"Инвестиции": 0.05, "Подушка безопасности": 0.05},
    "ХОЧУ": {"Развлечения": 0.07, "Отдых - путешествия": 0.05, "Покупки": 0.05},
}
ALL_CATEGORIES = [c for g in CATEGORIES.values() for c in g]
MAIN_BUTTONS = {"➕ Добавить трату", "📜 История", "📊 Моя статистика", "ℹ️ Помощь"}

class IncomeState(StatesGroup):
    income = State()

class ExpenseState(StatesGroup):
    amount = State()
    category = State()

class RecurringState(StatesGroup):
    amount = State()
    category = State()
    day = State()

# ---------------- Helpers & DB access ------------

async def ensure_user(uid: int):
    global db_lock
    if db_lock is None:
        db_lock = asyncio.Lock()
    async with db_lock:
        cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (uid,))
        conn.commit()

def get_income(uid: int) -> float:
    cursor.execute("SELECT income FROM users WHERE user_id = ?", (uid,))
    r = cursor.fetchone()
    return float(r["income"]) if r and r["income"] is not None else 0.0

def set_income(uid: int, v: float):
    cursor.execute("INSERT OR REPLACE INTO users (user_id, income) VALUES (?, ?)", (uid, v))
    conn.commit()

def format_amount(x):
    try:
        return f"{x:,.0f}".replace(",", " ")
    except Exception:
        return str(x)

def get_limits_from_income(income: float):
    return {cat: income * pct for group in CATEGORIES.values() for cat, pct in group.items()}

async def add_expense(uid, amount, category, ts=None, rec_id=None):
    ts = ts or datetime.now(TZ)
    global db_lock
    if db_lock is None:
        db_lock = asyncio.Lock()
    async with db_lock:
        cursor.execute("INSERT INTO expenses (user_id, amount, category, timestamp, recurring_id) VALUES (?, ?, ?, ?, ?)",
                       (uid, amount, category, ts.isoformat(), rec_id))
        conn.commit()

def get_expenses(uid, limit=10):
    cursor.execute("SELECT id, amount, category, timestamp FROM expenses WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?", (uid, limit))
    return cursor.fetchall()

def delete_expense(eid):
    cursor.execute("DELETE FROM expenses WHERE id = ?", (eid,))
    conn.commit()

def check_limits(uid, category, amount):
    limits = get_limits_from_income(get_income(uid))
    if category not in limits:
        return []
    income = get_income(uid)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    cursor.execute("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ?", (uid, month_start.isoformat(), month_end.isoformat()))
    total_spent = cursor.fetchone()["total"] or 0
    cursor.execute("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND category = ? AND timestamp BETWEEN ? AND ?", (uid, category, month_start.isoformat(), month_end.isoformat()))
    cat_spent = cursor.fetchone()["total"] or 0
    msgs = []
    if total_spent + amount > income:
        msgs.append("⚠️ Общий месячный лимит превышен!")
    if cat_spent + amount > limits[category]:
        msgs.append(f"⚠️ Лимит по '{category}' превышен!")
    elif cat_spent + amount > 0.9 * limits[category]:
        msgs.append(f"⚠️ Ты израсходовал более 90% лимита по '{category}'!")
    return msgs

def format_stats(uid: int) -> str:
    income = get_income(uid)
    limits = get_limits_from_income(income)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    cursor.execute("SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ? GROUP BY category",
                   (uid, month_start.isoformat(), month_end.isoformat()))
    rows = cursor.fetchall()
    spent = {r["category"]: r["total"] for r in rows}
    text = f"💰 Ваш доход: {format_amount(income)} ₽\n\n"
    for group, cats in CATEGORIES.items():
        text += f"📂 {group}\n"
        for cat, pct in cats.items():
            lim = limits.get(cat, 0)
            s = spent.get(cat, 0) or 0
            perc = (s / lim * 100) if lim else 0
            text += f"• {cat}: {s:,.0f} ₽ / {lim:,.0f} ₽ ({perc:.0f}%)\n"
        text += "\n"
    return text

# ---------------- Scheduler ----------------
scheduler = AsyncIOScheduler(timezone=TZ)

async def daily_reminders():
    cursor.execute("SELECT user_id FROM users WHERE notifications = 1")
    for (uid,) in cursor.fetchall():
        try:
            await bot.send_message(uid, "💡 Не забудь добавить траты за сегодня!")
        except Exception:
            logger.debug("Failed to send reminder to %s", uid)

async def weekly_report():
    cursor.execute("SELECT user_id FROM users")
    for (uid,) in cursor.fetchall():
        try:
            await bot.send_message(uid, "📊 Еженедельный отчёт:\n\n" + format_stats(uid))
        except Exception:
            logger.debug("Failed to send weekly report to %s", uid)

async def process_recurring():
    today = datetime.now(TZ).day
    cursor.execute("SELECT id, user_id, amount, category FROM recurring WHERE day = ?", (today,))
    for r in cursor.fetchall():
        rec_id, uid, amt, cat = r
        await add_expense(uid, amt, cat, rec_id=rec_id)
        try:
            await bot.send_message(uid, f"🔁 Добавлен регулярный расход: {amt:,.0f} ₽ — {cat}")
        except Exception:
            pass

def _add_scheduler_jobs_once():
    try:
        if not scheduler.get_job("daily_reminders"):
            scheduler.add_job(daily_reminders, CronTrigger(hour=9, minute=0), id="daily_reminders")
        if not scheduler.get_job("weekly_report"):
            scheduler.add_job(weekly_report, CronTrigger(day_of_week='mon', hour=9, minute=0), id="weekly_report")
        if not scheduler.get_job("process_recurring"):
            scheduler.add_job(process_recurring, CronTrigger(hour=6, minute=0), id="process_recurring")
    except Exception:
        logger.exception("Failed to add scheduler jobs")

# ---------------- UI helpers ----------------
def get_main_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("➕ Добавить трату", "📜 История")
    kb.add("📊 Моя статистика", "ℹ️ Помощь")
    return kb

def build_limits_table_html(income: float) -> str:
    limits = get_limits_from_income(income)
    lines = []
    lines.append(f"Доход: {format_amount(income)} ₽")
    lines.append("")
    lines.append("Рекомендуемые лимиты (процент / сумма):")
    lines.append("")
    max_cat_len = max(len(cat) for cat in limits.keys()) if limits else 0
    for group, cats in CATEGORIES.items():
        lines.append(f"{group}:")
        for cat, pct in cats.items():
            sum_rub = limits[cat]
            pct_str = f"{int(pct*100):>2}%"
            cat_name = cat.ljust(max_cat_len)
            sum_str = format_amount(sum_rub).rjust(10)
            lines.append(f"  {cat_name}   {pct_str}   {sum_str} ₽")
        lines.append("")
    pre_block = "<pre>" + "\n".join(lines) + "</pre>"
    return pre_block

# ---------------- Handlers (registered to dp) ----------------
@dp.message_handler(commands=['start'])
async def start(msg: types.Message):
    uid = msg.from_user.id
    await ensure_user(uid)
    welcome = (
        "<b>Привет! Я — твой финансовый помощник.</b>\n\n"
        "Я помогу тебе отслеживать расходы, планировать бюджет, "
        "настраивать регулярные платежи и вовремя предупреждать о превышениях лимитов.\n\n"
        "Чтобы начать — введите ваш ежемесячный доход (например: <b>50 000</b>)\n\n"
        "После ввода дохода я рассчитую рекомендованные лимиты по категориям и покажу подсказки по кнопкам внизу."
    )
    kb = get_main_keyboard()
    await IncomeState.income.set()
    await msg.reply(welcome, reply_markup=kb)

# ... (rest of handlers as in working version, omitted here for brevity in this snippet)
# In actual file include the complete handlers set you previously used (as in your working version).
# For clarity I assume you keep the previously provided full handler implementations.

# ------------- init helper -------------
async def init_app_for_runtime(app):
    """
    Called from main.py on startup to initialize db_lock, scheduler jobs, etc.
    """
    global db_lock
    if db_lock is None:
        db_lock = asyncio.Lock()

    _add_scheduler_jobs_once()
    try:
        scheduler.start()
        logger.info("Scheduler started (bot_app)")
    except Exception:
        logger.exception("Failed to start scheduler (bot_app)")

    # Optionally pre-create bot session (but main.py will also obtain session)
    try:
        sess = await bot.get_session()
        app['bot_session'] = sess
    except Exception:
        logger.debug("bot.get_session() failed during bot_app init (may be fine)")

    # ensure DB tables exist already (they are created at import but double-check)
    try:
        # Use a simple sync check inside executor if needed; here keep sync but guarded
        cursor.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, income REAL DEFAULT 0, notifications BOOLEAN DEFAULT 1)")
        cursor.execute("CREATE TABLE IF NOT EXISTS expenses (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, timestamp DATETIME, recurring_id INTEGER DEFAULT NULL)")
        cursor.execute("CREATE TABLE IF NOT EXISTS recurring (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, day INTEGER)")
        conn.commit()
    except Exception:
        logger.debug("DB ensure tables failed (ignored)")

# Export commonly used names
__all__ = ("bot", "dp", "scheduler", "init_app_for_runtime", "get_main_keyboard", "format_stats")
