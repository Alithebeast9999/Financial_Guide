# bot_app.py
import os
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
import pytz
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
    kb.row("📊 Моя статистика", "🎯 Цели")
    kb.row("📈 Аналитика", "ℹ️ Помощь")
    return kb

def get_amount_presets_inline():
    kb = InlineKeyboardMarkup(row_width=3)
    kb.row(
        InlineKeyboardButton("50", callback_data="preset_50"),
        InlineKeyboardButton("100", callback_data="preset_100"),
        InlineKeyboardButton("200", callback_data="preset_200"),
    )
    kb.row(
        InlineKeyboardButton("500", callback_data="preset_500"),
        InlineKeyboardButton("1000", callback_data="preset_1000"),
        InlineKeyboardButton("5000", callback_data="preset_5000"),
    )
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

def get_savings_keyboard():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.row(
        InlineKeyboardButton("🎯 Добавить цель", callback_data="savings_add"),
        InlineKeyboardButton("📊 Мои цели", callback_data="savings_list")
    )
    kb.row(
        InlineKeyboardButton("➕ Внести сумму", callback_data="savings_deposit"),
        InlineKeyboardButton("❌ Удалить цель", callback_data="savings_delete")
    )
    return kb

def get_limits_keyboard():
    kb = InlineKeyboardMarkup(row_width=2)
    for cat in ALL_CATEGORIES:
        kb.insert(InlineKeyboardButton(cat, callback_data=f"limit_{cat}"))
    kb.row(InlineKeyboardButton("📊 Показать все лимиты", callback_data="limits_show_all"))
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
    
    # Новые таблицы для дополнительных функций
    await db.execute("""CREATE TABLE IF NOT EXISTS category_limits (
        user_id INTEGER,
        category TEXT,
        limit_amount REAL,
        PRIMARY KEY (user_id, category)
    )""")
    
    await db.execute("""CREATE TABLE IF NOT EXISTS savings_goals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        name TEXT,
        target_amount REAL,
        current_amount REAL DEFAULT 0,
        deadline TEXT,
        created_at TEXT
    )""")
    
    await db.execute("CREATE INDEX IF NOT EXISTS idx_expenses_user_timestamp ON expenses(user_id, timestamp)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_recurring_day ON recurring(day)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_savings_user ON savings_goals(user_id)")
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
    # После добавления траты проверяем лимиты
    await check_and_notify_limits(uid, category, amount)

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

# Функция 1: Гибкие лимиты расходов
async def set_custom_limit(uid: int, category: str, limit: float):
    await ensure_user(uid)
    await db_execute(
        "INSERT OR REPLACE INTO category_limits (user_id, category, limit_amount) VALUES (?, ?, ?)",
        (uid, category, limit)
    )

async def get_category_limit(uid: int, category: str) -> Optional[float]:
    """Получить кастомный лимит или None если не установлен"""
    row = await db_fetchone(
        "SELECT limit_amount FROM category_limits WHERE user_id = ? AND category = ?",
        (uid, category)
    )
    return row["limit_amount"] if row else None

async def get_all_limits(uid: int) -> Dict[str, float]:
    """Получить все кастомные лимиты пользователя"""
    rows = await db_fetchall(
        "SELECT category, limit_amount FROM category_limits WHERE user_id = ?",
        (uid,)
    )
    return {row["category"]: row["limit_amount"] for row in rows}

# Функция 7: Система проверки лимитов
async def check_and_notify_limits(uid: int, category: str, added_amount: float):
    """Проверяет лимиты после добавления траты и отправляет уведомления"""
    income = await get_income(uid)
    if income <= 0:
        return
    
    now = datetime.utcnow()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Получаем потраченную сумму по категории за месяц
    row = await db_fetchone(
        """SELECT SUM(amount) as total FROM expenses 
           WHERE user_id = ? AND category = ? AND timestamp >= ?""",
        (uid, category, month_start.isoformat())
    )
    
    spent = row["total"] if row and row["total"] else 0
    
    # Получаем лимит (кастомный или расчетный)
    custom_limit = await get_category_limit(uid, category)
    if custom_limit:
        limit = custom_limit
    else:
        # Ищем категорию в стандартных
        for group_cats in CATEGORIES.values():
            if category in group_cats:
                limit = income * group_cats[category]
                break
        else:
            limit = None
    
    if limit:
        percentage = (spent / limit * 100) if limit > 0 else 0
        
        if percentage >= 100:
            await bot.send_message(
                uid,
                f"🚨 <b>Лимит превышен!</b>\n"
                f"Категория: {category}\n"
                f"Потрачено: {format_amount(spent)} ₽ из {format_amount(limit)} ₽ ({percentage:.1f}%)",
                parse_mode=types.ParseMode.HTML
            )
        elif percentage >= 80:
            await bot.send_message(
                uid,
                f"⚠️ <b>Приближение к лимиту</b>\n"
                f"Категория: {category}\n"
                f"Потрачено: {format_amount(spent)} ₽ из {format_amount(limit)} ₽ ({percentage:.1f}%)",
                parse_mode=types.ParseMode.HTML
            )

# Функция 2: Цели и накопления
async def add_savings_goal(uid: int, name: str, target_amount: float, deadline: str = None):
    created_at = datetime.utcnow().isoformat()
    await db_execute(
        """INSERT INTO savings_goals (user_id, name, target_amount, deadline, created_at) 
           VALUES (?, ?, ?, ?, ?)""",
        (uid, name, target_amount, deadline, created_at)
    )

async def update_savings_goal(goal_id: int, current_amount: float):
    await db_execute(
        "UPDATE savings_goals SET current_amount = ? WHERE id = ?",
        (current_amount, goal_id)
    )

async def get_savings_goals(uid: int) -> List[Dict[str, Any]]:
    rows = await db_fetchall(
        "SELECT id, name, target_amount, current_amount, deadline FROM savings_goals WHERE user_id = ?",
        (uid,)
    )
    return [dict(row) for row in rows]

async def delete_savings_goal(goal_id: int):
    await db_execute("DELETE FROM savings_goals WHERE id = ?", (goal_id,))

# Функция 3: Аналитика и визуализация
async def get_analytics_data(uid: int) -> Dict[str, Any]:
    """Получает данные для аналитики"""
    now = datetime.utcnow()
    
    # Статистика по дням недели
    weekly_stats = await db_fetchall("""
        SELECT strftime('%w', timestamp) as weekday, SUM(amount) as total 
        FROM expenses 
        WHERE user_id = ? AND timestamp >= date('now', '-30 days')
        GROUP BY weekday
        ORDER BY weekday
    """, (uid,))
    
    # Топ категорий
    top_categories = await db_fetchall("""
        SELECT category, COUNT(*) as count, SUM(amount) as total
        FROM expenses 
        WHERE user_id = ? 
        GROUP BY category 
        ORDER BY total DESC 
        LIMIT 5
    """, (uid,))
    
    # Средний чек
    avg_check = await db_fetchone("""
        SELECT AVG(amount) as avg_amount, COUNT(*) as count
        FROM expenses WHERE user_id = ?
    """, (uid,))
    
    return {
        "weekly_stats": weekly_stats,
        "top_categories": top_categories,
        "avg_check": avg_check
    }

# Функция 5: Умные напоминания
async def send_daily_reminders():
    """Отправляет ежедневные напоминания пользователям"""
    now = datetime.utcnow().date()
    
    # Получаем всех пользователей с включенными уведомлениями
    users = await db_fetchall(
        "SELECT user_id FROM users WHERE notifications = 1"
    )
    
    for user_row in users:
        uid = user_row["user_id"]
        try:
            # Проверяем, были ли сегодня траты
            today_expenses = await db_fetchone(
                "SELECT COUNT(*) as count FROM expenses WHERE user_id = ? AND date(timestamp) = ?",
                (uid, now.isoformat())
            )
            
            if today_expenses and today_expenses["count"] == 0:
                # Отправляем напоминание
                await bot.send_message(
                    uid,
                    "💡 <b>Добрый вечер!</b>\n\n"
                    "Вы еще не добавляли траты сегодня. Не забывайте вести учет расходов!",
                    parse_mode=types.ParseMode.HTML
                )
        except Exception as e:
            logger.debug(f"Error sending reminder to user {uid}: {e}")

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

# Новая команда: Аналитика
@dp.message_handler(commands=['analytics'])
async def analytics_cmd(msg: types.Message):
    uid = msg.from_user.id
    
    data = await get_analytics_data(uid)
    weekly_stats = data["weekly_stats"]
    top_categories = data["top_categories"]
    avg_check = data["avg_check"]
    
    text = "📈 <b>Аналитика расходов</b>\n\n"
    
    if weekly_stats:
        days = ["Вс", "Пн", "Вт", "Ср", "Чт", "Пт", "Сб"]
        text += "<b>Траты по дням недели (последние 30 дней):</b>\n"
        for stat in weekly_stats:
            day_name = days[int(stat["weekday"])]
            text += f"• {day_name}: {format_amount(stat['total'])} ₽\n"
        text += "\n"
    
    if top_categories:
        text += "<b>Топ категорий по расходам:</b>\n"
        for cat in top_categories:
            text += f"• {cat['category']}: {format_amount(cat['total'])} ₽ ({cat['count']} раз)\n"
        text += "\n"
    
    if avg_check and avg_check["avg_amount"]:
        text += f"<b>Средний чек:</b> {format_amount(avg_check['avg_amount'])} ₽\n"
        text += f"<b>Всего трат:</b> {avg_check['count']}\n"
    
    await bot.send_message(uid, text, parse_mode=types.ParseMode.HTML)

# Новая команда: Управление лимитами
@dp.message_handler(commands=['limits'])
async def limits_cmd(msg: types.Message):
    uid = msg.from_user.id
    
    await bot.send_message(
        uid,
        "🎯 <b>Управление лимитами расходов</b>\n\n"
        "Выберите категорию для настройки лимита:",
        parse_mode=types.ParseMode.HTML,
        reply_markup=get_limits_keyboard()
    )

# Новая команда: Цели накоплений
@dp.message_handler(commands=['savings'])
async def savings_cmd(msg: types.Message):
    uid = msg.from_user.id
    
    await bot.send_message(
        uid,
        "💰 <b>Управление целями накоплений</b>\n\n"
        "Здесь вы можете ставить цели и отслеживать прогресс накоплений:",
        parse_mode=types.ParseMode.HTML,
        reply_markup=get_savings_keyboard()
    )

@dp.message_handler(content_types=['text'])
async def generic_text_handler(msg: types.Message):
    uid = msg.from_user.id
    text = msg.text.strip()
    
    # Handle cancellation
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
    
    # Handle savings goal creation
    if pending and pending["type"] == "savings_name":
        goal_name = text
        await set_pending(uid, "savings_target", {"name": goal_name})
        await bot.send_message(uid, f"🎯 Цель: {goal_name}\n\nВведите сумму для накопления:")
        return
    
    if pending and pending["type"] == "savings_target":
        try:
            target_amount = float(text.replace(" ", "").replace(",", "."))
            goal_name = pending["data"]["name"]
            await add_savings_goal(uid, goal_name, target_amount)
            await pop_pending(uid)
            await bot.send_message(
                uid,
                f"✅ Цель добавлена!\n\n"
                f"🎯 <b>{goal_name}</b>\n"
                f"💰 Цель: {format_amount(target_amount)} ₽",
                parse_mode=types.ParseMode.HTML,
                reply_markup=get_main_keyboard()
            )
        except ValueError:
            await bot.send_message(uid, "❌ Неверный формат суммы. Введите число, например: 10000")
        return
    
    # Handle limit amount input
    if pending and pending["type"] == "limit_amount":
        try:
            limit_amount = float(text.replace(" ", "").replace(",", "."))
            category = pending["data"]["category"]
            await set_custom_limit(uid, category, limit_amount)
            await pop_pending(uid)
            await bot.send_message(
                uid,
                f"✅ Лимит установлен!\n\n"
                f"📊 Категория: {category}\n"
                f"🎯 Лимит: {format_amount(limit_amount)} ₽",
                parse_mode=types.ParseMode.HTML,
                reply_markup=get_main_keyboard()
            )
        except ValueError:
            await bot.send_message(uid, "❌ Неверный формат суммы. Введите число, например: 10000")
        return
    
    # Handle amount input flows
    if pending and pending["type"] in ["expense_amount", "recurring_amount"]:
        try:
            cleaned = text.replace(" ", "").replace(",", ".")
            amount = float(cleaned)
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
            pass
    
    # Main menu handlers
    if text == "➕ Добавить трату":
        sent = await bot.send_message(uid, "💸 Выберите сумму (пресеты):", reply_markup=get_amount_presets_inline())
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
        
        rows = await db_fetchall(
            "SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp >= ? GROUP BY category",
            (uid, month_start.isoformat())
        )
        
        spent = {r["category"]: (r["total"] or 0) for r in rows}
        total_spent = sum(spent.values())
        
        # Получаем кастомные лимиты
        custom_limits = await get_all_limits(uid)
        
        text = f"💰 Ваш доход: {format_amount(income)} ₽\n\n"
        for group, cats in CATEGORIES.items():
            text += f"📂 {group}\n"
            for cat, pct in cats.items():
                # Используем кастомный лимит или расчетный
                if cat in custom_limits:
                    lim = custom_limits[cat]
                    limit_source = " (кастомный)"
                else:
                    lim = income * pct
                    limit_source = ""
                
                s = spent.get(cat, 0)
                perc = (s / lim * 100) if lim else 0
                text += f"• {cat}: {format_amount(s)} ₽ / {format_amount(lim)} ₽{limit_source} ({perc:.0f}%)\n"
            text += "\n"
        
        text += f"📊 Всего потрачено: {format_amount(total_spent)} ₽ / {format_amount(income)} ₽ ({(total_spent/income*100) if income else 0:.0f}%)"
        
        await bot.send_message(uid, text)
        return
        
    elif text == "📈 Аналитика":
        await analytics_cmd(msg)
        return
        
    elif text == "🎯 Цели":
        await savings_cmd(msg)
        return
        
    elif text == "ℹ️ Помощь":
        help_text = (
            "📋 <b>Доступные команды:</b>\n\n"
            "➕ <b>Добавить трату</b> - быстро добавить расход\n"
            "📊 <b>Моя статистика</b> - текущее состояние бюджета\n"
            "📈 <b>Аналитика</b> - детальная аналитика расходов\n"
            "🎯 <b>Цели</b> - управление целями накоплений\n"
            "📜 <b>История</b> - последние траты\n\n"
            "<b>Команды:</b>\n"
            "/reportweek - отчёт за неделю\n"
            "/reportmonth - отчёт за месяц\n"
            "/analytics - расширенная аналитика\n"
            "/savings - цели накоплений\n"
            "/limits - управление лимитами\n"
            "/add_recurring - добавить регулярный расход\n"
            "/start - перезапустить бота"
        )
        await bot.send_message(uid, help_text, parse_mode=types.ParseMode.HTML)
        return
    
    await bot.send_message(uid, "Не понял. Используйте кнопки ниже.", reply_markup=get_main_keyboard())

# Заменяем существующий callback_handler на этот исправленный код:

@dp.callback_query_handler(lambda c: c.data and (c.data.startswith('preset_') or c.data.startswith('cat_') or 
                                                 c.data.startswith('rec_') or c.data.startswith('del_') or
                                                 c.data.startswith('savings_') or c.data.startswith('deposit_') or
                                                 c.data.startswith('deletegoal_') or c.data.startswith('limit_')))
async def callback_handler(cb: types.CallbackQuery):
    uid = cb.from_user.id
    data = cb.data

    # Preset buttons - ВАЖНО: эта проверка должна быть первой!
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

        try:
            amount = float(key)
        except Exception:
            await cb.answer()
            return

        pending = await get_pending(uid)
        if pending and pending["type"] == "recurring_amount":
            await set_pending(uid, "recurring_choose_category", {"amount": amount})
            kb = InlineKeyboardMarkup(row_width=2)
            for cat in ALL_CATEGORIES:
                kb.insert(InlineKeyboardButton(cat, callback_data=f"rec_{cat}"))
            try:
                await cb.message.edit_text(f"💸 Сумма регулярного расхода: {format_amount(amount)} ₽\n\nВыбери категорию:", reply_markup=kb)
            except Exception:
                await bot.send_message(uid, f"💸 Сумма регулярного расхода: {format_amount(amount)} ₽\n\nВыбери категорию:", reply_markup=kb)
        else:
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

    # Deposit to savings goal - ВАЖНО: эта проверка должна быть ДО savings_
    if data.startswith("deposit_"):
        try:
            goal_id = int(data[8:])
            await set_pending(uid, "savings_deposit", {"goal_id": goal_id})
            await cb.message.edit_text("💰 Введите сумму для внесения:")
        except Exception as e:
            logger.error(f"Error in deposit callback: {e}")
            await cb.message.edit_text("❌ Ошибка выбора цели.")
        await cb.answer()
        return

    # Delete savings goal - ВАЖНО: эта проверка должна быть ДО savings_
    if data.startswith("deletegoal_"):
        try:
            goal_id = int(data[11:])
            await delete_savings_goal(goal_id)
            await cb.message.edit_text("✅ Цель удалена.")
        except Exception as e:
            logger.error(f"Error deleting goal: {e}")
            await cb.message.edit_text("❌ Ошибка удаления цели.")
        await cb.answer()
        return

    # Expense category selection
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

    # Recurring category selection
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
        except Exception as e:
            logger.error(f"Error deleting expense: {e}")
            await cb.answer("Ошибка удаления")
        return

    # Savings goals management - ВАЖНО: эта проверка должна быть ПОСЛЕ deposit_ и deletegoal_
    if data.startswith("savings_"):
        action = data[8:]
        
        if action == "add":
            await set_pending(uid, "savings_name")
            await cb.message.edit_text("🎯 Введите название цели накопления:")
            
        elif action == "list":
            goals = await get_savings_goals(uid)
            if not goals:
                await cb.message.edit_text("🎯 У вас пока нет целей накопления.")
            else:
                text = "🎯 <b>Ваши цели накопления:</b>\n\n"
                for goal in goals:
                    progress = (goal["current_amount"] / goal["target_amount"] * 100) if goal["target_amount"] > 0 else 0
                    deadline_text = f" до {goal['deadline']}" if goal["deadline"] else ""
                    text += (f"• <b>{goal['name']}</b>{deadline_text}\n"
                           f"  {format_amount(goal['current_amount'])} / {format_amount(goal['target_amount'])} ₽ "
                           f"({progress:.1f}%)\n\n")
                await cb.message.edit_text(text, parse_mode=types.ParseMode.HTML)
                
        elif action == "deposit":
            goals = await get_savings_goals(uid)
            if not goals:
                await cb.message.edit_text("❌ У вас нет целей для пополнения.")
            else:
                kb = InlineKeyboardMarkup(row_width=1)
                for goal in goals:
                    kb.insert(InlineKeyboardButton(
                        goal["name"], 
                        callback_data=f"deposit_{goal['id']}"
                    ))
                await cb.message.edit_text("Выберите цель для пополнения:", reply_markup=kb)
                
        elif action == "delete":
            goals = await get_savings_goals(uid)
            if not goals:
                await cb.message.edit_text("❌ У вас нет целей для удаления.")
            else:
                kb = InlineKeyboardMarkup(row_width=1)
                for goal in goals:
                    kb.insert(InlineKeyboardButton(
                        goal["name"], 
                        callback_data=f"deletegoal_{goal['id']}"
                    ))
                await cb.message.edit_text("Выберите цель для удаления:", reply_markup=kb)
        
        await cb.answer()
        return

    # Limit management
    if data.startswith("limit_"):
        if data == "limits_show_all":
            custom_limits = await get_all_limits(uid)
            income = await get_income(uid)
            
            if not custom_limits:
                text = "📊 <b>Ваши лимиты:</b>\n\n"
                text += "Кастомные лимиты не установлены. Используются расчетные лимиты:\n\n"
            else:
                text = "📊 <b>Ваши лимиты:</b>\n\n"
                text += "<b>Кастомные лимиты:</b>\n"
                for cat, limit in custom_limits.items():
                    text += f"• {cat}: {format_amount(limit)} ₽\n"
                text += "\n<b>Расчетные лимиты:</b>\n"
            
            # Показываем расчетные лимиты для категорий без кастомных
            for group, cats in CATEGORIES.items():
                for cat, pct in cats.items():
                    if cat not in custom_limits:
                        limit = income * pct if income > 0 else 0
                        text += f"• {cat}: {format_amount(limit)} ₽\n"
            
            await cb.message.edit_text(text, parse_mode=types.ParseMode.HTML)
        else:
            category = data[6:]
            await set_pending(uid, "limit_amount", {"category": category})
            income = await get_income(uid)
            
            # Рассчитываем рекомендуемый лимит
            recommended = 0
            for group_cats in CATEGORIES.values():
                if category in group_cats:
                    recommended = income * group_cats[category] if income > 0 else 0
                    break
            
            await cb.message.edit_text(
                f"📊 Установка лимита для категории: <b>{category}</b>\n\n"
                f"Рекомендуемый лимит: {format_amount(recommended)} ₽\n"
                f"Введите сумму лимита:",
                parse_mode=types.ParseMode.HTML
            )
        await cb.answer()
        return

    # Если ни одно условие не сработало
    await cb.answer("Неизвестная команда")

# Обработчик для регулярных платежей (день месяца)
@dp.message_handler(lambda msg: msg.text.isdigit() and 1 <= int(msg.text) <= 31)
async def handle_recurring_day(msg: types.Message):
    uid = msg.from_user.id
    pending = await get_pending(uid)
    
    if pending and pending["type"] == "recurring_day":
        day = int(msg.text)
        amount = pending["data"]["amount"]
        category = pending["data"]["category"]
        
        await db_execute(
            "INSERT INTO recurring (user_id, amount, category, day) VALUES (?, ?, ?, ?)",
            (uid, amount, category, day)
        )
        
        await pop_pending(uid)
        await bot.send_message(
            uid, 
            f"✅ Регулярный расход добавлен:\n"
            f"• Сумма: {format_amount(amount)} ₽\n"
            f"• Категория: {category}\n"
            f"• День месяца: {day}",
            reply_markup=get_main_keyboard()
        )

# Замените существующий обработчик handle_savings_deposit на этот:

@dp.message_handler(lambda msg: msg.text.replace(" ", "").replace(",", ".").replace(".", "", 1).isdigit())
async def handle_savings_deposit(msg: types.Message):
    uid = msg.from_user.id
    pending = await get_pending(uid)
    
    if pending and pending["type"] == "savings_deposit":
        try:
            amount = float(msg.text.replace(" ", "").replace(",", "."))
            goal_id = pending["data"]["goal_id"]
            
            # Получаем текущую сумму цели
            goal = await db_fetchone("SELECT current_amount, name, target_amount FROM savings_goals WHERE id = ?", (goal_id,))
            if goal:
                new_amount = goal["current_amount"] + amount
                await update_savings_goal(goal_id, new_amount)
                await pop_pending(uid)
                
                progress = (new_amount / goal["target_amount"] * 100) if goal["target_amount"] > 0 else 0
                await bot.send_message(
                    uid,
                    f"✅ Внесено {format_amount(amount)} ₽ в цель '{goal['name']}'\n\n"
                    f"Всего накоплено: {format_amount(new_amount)} ₽ из {format_amount(goal['target_amount'])} ₽\n"
                    f"Прогресс: {progress:.1f}%",
                    reply_markup=get_main_keyboard()
                )
            else:
                await bot.send_message(uid, "❌ Цель не найдена.", reply_markup=get_main_keyboard())
                await pop_pending(uid)
        except ValueError:
            await bot.send_message(uid, "❌ Неверный формат суммы.")
        except Exception as e:
            logger.error(f"Error depositing to savings: {e}")
            await bot.send_message(uid, "❌ Ошибка при внесении средств.", reply_markup=get_main_keyboard())
            await pop_pending(uid)

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
        # Добавляем задачу для умных напоминаний (каждый день в 20:00 по Москве)
        scheduler.add_job(send_daily_reminders, CronTrigger(hour=20, minute=0, timezone=TZ), id="daily_reminders")
    except Exception as e:
        logger.exception(f"Failed to add scheduler jobs: {e}")

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
