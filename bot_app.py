# bot_app.py
import os
import logging
import asyncio
from datetime import datetime, timedelta
import pytz
from typing import Dict, Any, Optional
import aiosqlite
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

logger = logging.getLogger(__name__)

# Config
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

TZ = pytz.timezone("Europe/Moscow")
bot = Bot(token=BOT_TOKEN, timeout=30, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Database
DB_FILE = "bot.db"
db: Optional[aiosqlite.Connection] = None
db_lock: Optional[asyncio.Lock] = None

# Pending actions
pending_actions: Dict[int, Dict[str, Any]] = {}
pending_lock = asyncio.Lock()

# Categories
CATEGORIES = {
    "НАДО": {"Аренда жилья": 0.35, "Продуктовая корзина": 0.15, "Комм. услуги": 0.05, "Связь": 0.03, "Транспорт": 0.05, "Личный уход": 0.02, "Медицина": 0.08},
    "МОГУ": {"Инвестиции": 0.05, "Подушка безопасности": 0.05},
    "ХОЧУ": {"Развлечения": 0.07, "Отдых - путешествия": 0.05, "Покупки": 0.05},
}
ALL_CATEGORIES = [c for g in CATEGORIES.values() for c in g]

# Keyboards
def get_main_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("➕ Добавить трату", "📜 История")
    kb.row("📊 Моя статистика", "ℹ️ Помощь")
    return kb

def get_amount_presets_inline():
    """Inline keyboard with presets + cancel"""
    kb = InlineKeyboardMarkup(row_width=3)
    # Preset buttons (small values)
    kb.row(
        InlineKeyboardButton("50", callback_data="preset_50"),
        InlineKeyboardButton("100", callback_data="preset_100"),
        InlineKeyboardButton("200", callback_data="preset_200"),
    )
    # Medium values
    kb.row(
        InlineKeyboardButton("500", callback_data="preset_500"),
        InlineKeyboardButton("1000", callback_data="preset_1000"),
        InlineKeyboardButton("5000", callback_data="preset_5000"),
    )
    # Large value + cancel
    kb.row(
        InlineKeyboardButton("10000", callback_data="preset_10000"),
        InlineKeyboardButton("❌ Отмена", callback_data="preset_cancel"),
    )
    return kb

def get_days_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.row("1", "2", "3", "4", "5")
    kb.row("6", "7", "8", "9", "10")
    kb.row("11", "12", "13", "14", "15")
    kb.row("16", "17", "18", "19", "20")
    kb.row("21", "22", "23", "24", "25")
    kb.row("26", "27", "28", "29", "30")
    kb.row("31", "❌ Отмена")
    return kb

# Database helpers
async def init_db():
    global db
    db = await aiosqlite.connect(DB_FILE)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA foreign_keys=ON;")
    
    await db.execute("""CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        income REAL DEFAULT 0,
        notifications BOOLEAN DEFAULT 1,
        first_name TEXT,
        username TEXT,
        created_at TEXT,
        last_active TEXT
    )""")
    
    await db.execute("""CREATE TABLE IF NOT EXISTS expenses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        category TEXT,
        timestamp TEXT,
        recurring_id INTEGER DEFAULT NULL
    )""")
    
    await db.execute("""CREATE TABLE IF NOT EXISTS recurring (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        category TEXT,
        day INTEGER
    )""")
    
    await db.execute("CREATE INDEX IF NOT EXISTS idx_expenses_user_timestamp ON expenses(user_id, timestamp)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_recurring_day ON recurring(day)")
    await db.commit()

async def close_db():
    global db
    if db:
        await db.close()
        db = None

async def db_execute(query: str, params: tuple = ()):
    if not db:
        raise RuntimeError("DB not initialized")
    async with db_lock:
        await db.execute(query, params)
        await db.commit()

async def db_fetchone(query: str, params: tuple = ()):
    if not db:
        raise RuntimeError("DB not initialized")
    async with db_lock:
        cur = await db.execute(query, params)
        row = await cur.fetchone()
        await cur.close()
        return row

async def db_fetchall(query: str, params: tuple = ()):
    if not db:
        raise RuntimeError("DB not initialized")
    async with db_lock:
        cur = await db.execute(query, params)
        rows = await cur.fetchall()
        await cur.close()
        return rows

# Business logic helpers
async def ensure_user(uid: int, first_name: str = "", username: str = ""):
    global db_lock
    if not db_lock:
        db_lock = asyncio.Lock()
    
    now = datetime.utcnow().isoformat()
    existing = await db_fetchone("SELECT user_id FROM users WHERE user_id = ?", (uid,))
    
    if existing:
        await db_execute("UPDATE users SET last_active = ? WHERE user_id = ?", (now, uid))
    else:
        await db_execute(
            "INSERT INTO users (user_id, first_name, username, created_at, last_active) VALUES (?, ?, ?, ?, ?)",
            (uid, first_name, username, now, now)
        )

async def get_income(uid: int) -> float:
    r = await db_fetchone("SELECT income FROM users WHERE user_id = ?", (uid,))
    return float(r["income"]) if r and r["income"] is not None else 0.0

async def set_income(uid: int, v: float):
    await ensure_user(uid)
    await db_execute("UPDATE users SET income = ? WHERE user_id = ?", (v, uid))

def format_amount(x):
    try:
        # show integer formatting with spaces
        if isinstance(x, float) and not x.is_integer():
            return f"{x:,.2f}".replace(",", " ")
        return f"{int(x):,}".replace(",", " ")
    except Exception:
        return str(x)

async def add_expense(uid, amount, category, ts=None, rec_id=None):
    ts = ts or datetime.utcnow().isoformat()
    await db_execute(
        "INSERT INTO expenses (user_id, amount, category, timestamp, recurring_id) VALUES (?, ?, ?, ?, ?)",
        (uid, amount, category, ts, rec_id)
    )

async def get_expenses(uid, limit=10):
    return await db_fetchall(
        "SELECT id, amount, category, timestamp FROM expenses WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?",
        (uid, limit)
    )

async def delete_expense(eid):
    await db_execute("DELETE FROM expenses WHERE id = ?", (eid,))

# Pending actions management
async def set_pending(uid: int, action_type: str, data: Optional[Dict[str, Any]] = None):
    async with pending_lock:
        pending_actions[uid] = {"type": action_type, "data": data or {}}

async def pop_pending(uid: int) -> Optional[Dict[str, Any]]:
    async with pending_lock:
        return pending_actions.pop(uid, None)

async def get_pending(uid: int) -> Optional[Dict[str, Any]]:
    async with pending_lock:
        return pending_actions.get(uid)

# Handlers
@dp.message_handler(commands=['start'])
async def start(msg: types.Message):
    uid = msg.from_user.id
    first_name = msg.from_user.first_name or ""
    
    await ensure_user(uid, first_name, msg.from_user.username or "")
    income = await get_income(uid)
    
    if income > 0:
        welcome = (
            f"<b>С возвращением, {first_name}! 👋</b>\n\n"
            f"Рад снова видеть тебя! Продолжим оптимизировать твои финансы?\n\n"
            f"Твой доход: {format_amount(income)} ₽\n\n"
            f"Используй кнопки ниже для управления бюджетом!"
        )
    else:
        welcome = (
            "<b>Привет! Я — твой финансовый помощник. 🤖💰</b>\n\n"
            "Я помогу тебе отслеживать расходы, планировать бюджет и вовремя предупреждать о превышениях лимитов.\n\n"
            "<b>Чтобы начать — введи свой ежемесячный доход</b> (например: <b>50 000</b>)"
        )
        await set_pending(uid, "income")
    
    await bot.send_message(msg.chat.id, welcome, reply_markup=get_main_keyboard(), parse_mode=types.ParseMode.HTML)

@dp.message_handler(commands=['reportweek'])
async def report_week_cmd(msg: types.Message):
    week_start = datetime.utcnow() - timedelta(days=7)
    rows = await db_fetchall(
        "SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp >= ? GROUP BY category",
        (msg.from_user.id, week_start.isoformat())
    )
    
    if not rows:
        await bot.send_message(msg.chat.id, "📊 <b>Отчёт за неделю</b>\n\nНет данных за последние 7 дней.", parse_mode=types.ParseMode.HTML)
        return
    
    total_spent = sum(r["total"] for r in rows if r["total"])
    text = "📊 <b>Отчёт за неделю</b>\n\n" + "\n".join(
        f"• {r['category']}: {format_amount(r['total'])} ₽" for r in rows
    ) + f"\n\n<b>Итого:</b> {format_amount(total_spent)} ₽"
    
    await bot.send_message(msg.chat.id, text, parse_mode=types.ParseMode.HTML)

@dp.message_handler(commands=['reportmonth'])
async def report_month_cmd(msg: types.Message):
    now = datetime.utcnow()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    month_end = next_month - timedelta(seconds=1)
    
    rows = await db_fetchall(
        "SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ? GROUP BY category",
        (msg.from_user.id, month_start.isoformat(), month_end.isoformat())
    )
    
    if not rows:
        await bot.send_message(msg.chat.id, "📊 <b>Отчёт за месяц</b>\n\nНет данных за текущий месяц.", parse_mode=types.ParseMode.HTML)
        return
    
    total_spent = sum(r["total"] for r in rows if r["total"])
    text = f"📊 <b>Отчёт за {month_start.strftime('%B')}</b>\n\n" + "\n".join(
        f"• {r['category']}: {format_amount(r['total'])} ₽" for r in rows
    ) + f"\n\n<b>Итого:</b> {format_amount(total_spent)} ₽"
    
    await bot.send_message(msg.chat.id, text, parse_mode=types.ParseMode.HTML)

@dp.message_handler(content_types=['text'])
async def generic_text_handler(msg: types.Message):
    uid = msg.from_user.id
    text = msg.text.strip()
    
    # Handle cancellation (text cancel)
    if text == "❌ Отмена":
        await pop_pending(uid)
        await bot.send_message(uid, "❌ Действие отменено.", reply_markup=get_main_keyboard())
        return
    
    pending = await get_pending(uid)
    
    # Handle income input
    if pending and pending["type"] == "income":
        try:
            income = float(text.replace(" ", "").replace(",", "."))
            await set_income(uid, income)
            await pop_pending(uid)
            
            limits_text = f"<b>Доход установлен:</b> {format_amount(income)} ₽\n\n<b>Рекомендуемые лимиты:</b>\n"
            for group, cats in CATEGORIES.items():
                limits_text += f"\n<b>{group}:</b>\n"
                for cat, pct in cats.items():
                    limit = income * pct
                    limits_text += f"• {cat}: {format_amount(limit)} ₽\n"
            
            await bot.send_message(uid, limits_text, parse_mode=types.ParseMode.HTML, reply_markup=get_main_keyboard())
        except ValueError:
            await bot.send_message(uid, "❌ Неверный формат дохода. Введите число, например: 50000")
        return
    
    # If user typed a number while in amount-input flow (manual input), accept it
    if pending and pending["type"] in ["expense_amount", "recurring_amount"]:
        # try parse direct full-number input (manual)
        try:
            cleaned = text.replace(" ", "").replace(",", ".")
            amount = float(cleaned)
            # proceed similarly to pressing a preset/ok
            if pending["type"] == "expense_amount":
                await set_pending(uid, "expense_choose_category", {"amount": amount})
                kb = InlineKeyboardMarkup(row_width=2)
                for cat in ALL_CATEGORIES:
                    kb.insert(InlineKeyboardButton(cat, callback_data=f"cat_{cat}"))
                await bot.send_message(uid, f"💸 Сумма: {format_amount(amount)} ₽\n\nВыбери категорию:", reply_markup=kb)
            else:
                await set_pending(uid, "recurring_choose_category", {"amount": amount})
                kb = InlineKeyboardMarkup(row_width=2)
                for cat in ALL_CATEGORIES:
                    kb.insert(InlineKeyboardButton(cat, callback_data=f"rec_{cat}"))
                await bot.send_message(uid, f"💸 Сумма: {format_amount(amount)} ₽\n\nВыбери категорию:", reply_markup=kb)
            return
        except Exception:
            # Not a numeric message -> fall through
            pass
    
    # Main menu handlers
    if text == "➕ Добавить трату":
        # send presets inline keyboard
        sent = await bot.send_message(uid, "💸 Выберите сумму (пресеты):", reply_markup=get_amount_presets_inline())
        # set pending for amount. awaiting_manual removed — manual input still works if user types a number
        await set_pending(uid, "expense_amount", {"msg_id": sent.message_id, "chat_id": sent.chat.id})
        return
        
    elif text == "📜 История":
        exps = await get_expenses(uid)
        if not exps:
            await bot.send_message(uid, "Пока нет трат 💰")
            return
        for e in exps:
            ts = e['timestamp']
            try:
                dt = datetime.fromisoformat(ts).strftime('%d.%m %H:%M')
            except:
                dt = ts
            kb = InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Удалить", callback_data=f"del_{e['id']}"))
            await bot.send_message(uid, f"{dt} | {format_amount(e['amount'])} ₽ | {e['category']}", reply_markup=kb)
        return
        
    elif text == "📊 Моя статистика":
        income = await get_income(uid)
        if income <= 0:
            await bot.send_message(uid, "❌ Сначала установите доход через /start")
            return
            
        now = datetime.utcnow()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        next_month = (month_start + timedelta(days=32)).replace(day=1)
        month_end = next_month - timedelta(seconds=1)
        
        rows = await db_fetchall(
            "SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ? GROUP BY category",
            (uid, month_start.isoformat(), month_end.isoformat())
        )
        
        spent = {r["category"]: (r["total"] or 0) for r in rows}
        total_spent = sum(spent.values())
        
        text = f"💰 Ваш доход: {format_amount(income)} ₽\n\n"
        for group, cats in CATEGORIES.items():
            text += f"📂 {group}\n"
            for cat, pct in cats.items():
                lim = income * pct
                s = spent.get(cat, 0)
                perc = (s / lim * 100) if lim else 0
                text += f"• {cat}: {format_amount(s)} ₽ / {format_amount(lim)} ₽ ({perc:.0f}%)\n"
            text += "\n"
        text += f"📊 Всего потрачено: {format_amount(total_spent)} ₽ / {format_amount(income)} ₽ ({(total_spent/income*100) if income else 0:.0f}%)"
        
        await bot.send_message(uid, text)
        return
        
    elif text == "ℹ️ Помощь":
        help_text = (
            "📋 <b>Доступные команды:</b>\n\n"
            "➕ <b>Добавить трату</b> - быстро добавить расход\n"
            "📊 <b>Моя статистика</b> - текущее состояние бюджета\n"
            "📜 <b>История</b> - последние траты\n\n"
            "<b>Команды:</b>\n"
            "/reportweek - отчёт за неделю\n"
            "/reportmonth - отчёт за месяц\n"
            "/add_recurring - добавить регулярный расход\n"
            "/start - перезапустить бота"
        )
        await bot.send_message(uid, help_text, parse_mode=types.ParseMode.HTML)
        return
    
    await bot.send_message(uid, "Не понял. Используйте кнопки ниже.", reply_markup=get_main_keyboard())

# Callback handlers for presets and category selection
@dp.callback_query_handler(lambda c: c.data and (c.data.startswith('preset_') or c.data.startswith('cat_') or c.data.startswith('rec_') or c.data.startswith('del_')))
async def presets_and_categories(cb: types.CallbackQuery):
    uid = cb.from_user.id
    data = cb.data

    # Preset pressed: e.g. preset_100
    if data.startswith("preset_"):
        key = data.split("_", 1)[1]
        if key == "cancel":
            await pop_pending(uid)
            try:
                await cb.message.edit_text("❌ Действие отменено.")
            except:
                pass
            await bot.send_message(uid, "Используйте кнопки ниже для продолжения:", reply_markup=get_main_keyboard())
            await cb.answer()
            return

        # numeric preset
        try:
            amount = float(key)
        except Exception:
            await cb.answer()
            return

        pending = await get_pending(uid)
        # Determine whether this was recurring flow or expense flow
        if pending and pending["type"] == "recurring_amount":
            # go to recurring_choose_category
            await set_pending(uid, "recurring_choose_category", {"amount": amount})
            kb = InlineKeyboardMarkup(row_width=2)
            for cat in ALL_CATEGORIES:
                kb.insert(InlineKeyboardButton(cat, callback_data=f"rec_{cat}"))
            try:
                await cb.message.edit_text(f"💸 Сумма регулярного расхода: {format_amount(amount)} ₽\n\nВыбери категорию:", reply_markup=kb)
            except Exception:
                await bot.send_message(uid, f"💸 Сумма регулярного расхода: {format_amount(amount)} ₽\n\nВыбери категорию:", reply_markup=kb)
            await cb.answer()
            return
        else:
            # expense flow
            await set_pending(uid, "expense_choose_category", {"amount": amount})
            kb = InlineKeyboardMarkup(row_width=2)
            for cat in ALL_CATEGORIES:
                kb.insert(InlineKeyboardButton(cat, callback_data=f"cat_{cat}"))
            try:
                await cb.message.edit_text(f"💸 Сумма: {format_amount(amount)} ₽\n\nВыбери категорию:", reply_markup=kb)
            except Exception:
                await bot.send_message(uid, f"💸 Сумма: {format_amount(amount)} ₽\n\nВыбери категорию:", reply_markup=kb)
            await cb.answer()
            return

    # Category selection for expense
    if data.startswith("cat_"):
        cat = data[4:]
        pending = await get_pending(uid)
        if pending and pending["type"] == "expense_choose_category":
            amount = pending["data"]["amount"]
            await add_expense(uid, amount, cat)
            await pop_pending(uid)
            try:
                await cb.message.edit_text(f"✅ Добавлено: {format_amount(amount)} ₽ — {cat}")
            except:
                pass
            await bot.send_message(uid, "Используйте кнопки ниже для продолжения:", reply_markup=get_main_keyboard())
        await cb.answer()
        return

    # Category selection for recurring
    if data.startswith("rec_"):
        cat = data[4:]
        pending = await get_pending(uid)
        if pending and pending["type"] == "recurring_choose_category":
            amount = pending["data"]["amount"]
            await set_pending(uid, "recurring_day", {"amount": amount, "category": cat})
            try:
                await cb.message.edit_text("Укажи день месяца (1–31):")
            except:
                pass
            await bot.send_message(uid, "Укажи день месяца (1–31):", reply_markup=get_days_keyboard())
        await cb.answer()
        return

    # Delete expense
    if data.startswith("del_"):
        try:
            eid = int(data[4:])
            await delete_expense(eid)
            await cb.answer("Удалено")
            try:
                await cb.message.delete()
            except:
                pass
        except:
            await cb.answer()
        return

    await cb.answer()

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('del_'))
async def delete_expense_cb(cb: types.CallbackQuery):
    eid = int(cb.data[4:])
    await delete_expense(eid)
    await cb.answer("Удалено")
    try:
        await cb.message.delete()
    except:
        pass

@dp.message_handler(commands=['add_recurring'])
async def add_recurring(msg: types.Message):
    uid = msg.from_user.id
    sent = await bot.send_message(uid, "💸 Выберите сумму для регулярного расхода (пресеты):", reply_markup=get_amount_presets_inline())
    await set_pending(uid, "recurring_amount", {"msg_id": sent.message_id, "chat_id": sent.chat.id})

# Scheduler
scheduler = AsyncIOScheduler(timezone=TZ)

def get_last_day_of_month(year: int, month: int) -> int:
    if month == 12:
        return 31
    return (datetime(year, month + 1, 1) - timedelta(days=1)).day

async def process_recurring():
    now = datetime.utcnow()
    current_day = now.day
    last_day = get_last_day_of_month(now.year, now.month)
    
    rows = await db_fetchall("SELECT id, user_id, amount, category, day FROM recurring")
    
    for r in rows:
        try:
            scheduled_day = min(r["day"], last_day)
            if current_day == scheduled_day:
                await add_expense(r["user_id"], r["amount"], r["category"], rec_id=r["id"])
                try:
                    day_info = f" (скорректировано с {r['day']}-го)" if r["day"] > last_day else ""
                    await bot.send_message(r["user_id"], f"🔁 Добавлен регулярный расход: {format_amount(r['amount'])} ₽ — {r['category']}{day_info}")
                except:
                    pass
        except Exception as e:
            logger.debug(f"Recurring expense error: {e}")

def _add_scheduler_jobs():
    try:
        scheduler.add_job(process_recurring, CronTrigger(hour=6, minute=0), id="process_recurring")
    except:
        logger.exception("Failed to add scheduler jobs")

# App initialization
async def init_app_for_runtime(app):
    global db_lock
    if not db_lock:
        db_lock = asyncio.Lock()
    await init_db()
    _add_scheduler_jobs()
    try:
        scheduler.start()
    except:
        logger.exception("Failed to start scheduler")

__all__ = ("bot", "dp", "scheduler", "init_app_for_runtime", "get_main_keyboard", "close_db")
