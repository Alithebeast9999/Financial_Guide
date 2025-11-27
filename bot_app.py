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
TZ = pytz.timezone("Europe/Moscow") # used for display; DB stores UTC timestamps
# ---------------- Bot / Dispatcher ---------------
bot = Bot(token=BOT_TOKEN, timeout=30, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
# ---------------- DB (aiosqlite) --------------------
DB_FILE = "bot.db"
db: Optional[aiosqlite.Connection] = None
# db_lock for async coordination
db_lock: Optional[asyncio.Lock] = None
# ---------------- Pending actions (conversation shim, PENDING) --------------
pending_actions: Dict[int, Dict[str, Any]] = {}
pending_lock = asyncio.Lock()
async def set_pending(uid: int, action_type: str, data: Optional[Dict[str, Any]] = None):
    async with pending_lock:
        pending_actions[uid] = {"type": action_type, "data": data or {}}
        logger.info("PENDING: set for %s -> %s", uid, action_type)
async def pop_pending(uid: int) -> Optional[Dict[str, Any]]:
    async with pending_lock:
        return pending_actions.pop(uid, None)
async def get_pending(uid: int) -> Optional[Dict[str, Any]]:
    async with pending_lock:
        return pending_actions.get(uid)
# ---------------- Categories & states -------------
CATEGORIES = {
    "НАДО": {"Аренда жилья": 0.35, "Продуктовая корзина": 0.15, "Комм. услуги": 0.05, "Связь": 0.03, "Транспорт": 0.05, "Личный уход": 0.02, "Медицина": 0.08},
    "МОГУ": {"Инвестиции": 0.05, "Подушка безопасности": 0.05},
    "ХОЧУ": {"Развлечения": 0.07, "Отдых - путешествия": 0.05, "Покупки": 0.05},
}
ALL_CATEGORIES = [c for g in CATEGORIES.values() for c in g]
MAIN_BUTTONS = {"➕ Добавить трату", "📜 История", "📊 Моя статистика", "ℹ️ Помощь"}
# ---------------- Keyboard helpers ----------------
def get_main_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("➕ Добавить трату", "📜 История")
    kb.add("📊 Моя статистика", "ℹ️ Помощь")
    return kb

def get_cancel_keyboard():
    """Клавиатура только с кнопкой отмены"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add("❌ Отмена")
    return kb

def get_digits_keyboard():
    """Цифровая клавиатура для ввода суммы с кнопками управления"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    row1 = [KeyboardButton("1"), KeyboardButton("2"), KeyboardButton("3")]
    row2 = [KeyboardButton("4"), KeyboardButton("5"), KeyboardButton("6")]
    row3 = [KeyboardButton("7"), KeyboardButton("8"), KeyboardButton("9")]
    row4 = [KeyboardButton("0"), KeyboardButton("🗑️ Очистить")]
    row5 = [KeyboardButton("✅ Готово"), KeyboardButton("❌ Отмена")]
    kb.add(*row1)
    kb.add(*row2)
    kb.add(*row3)
    kb.add(*row4)
    kb.add(*row5)
    return kb

def get_days_keyboard():
    """Клавиатура для выбора дня месяца"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    row1 = [KeyboardButton("1"), KeyboardButton("2"), KeyboardButton("3"), KeyboardButton("4"), KeyboardButton("5")]
    row2 = [KeyboardButton("6"), KeyboardButton("7"), KeyboardButton("8"), KeyboardButton("9"), KeyboardButton("10")]
    row3 = [KeyboardButton("11"), KeyboardButton("12"), KeyboardButton("13"), KeyboardButton("14"), KeyboardButton("15")]
    row4 = [KeyboardButton("16"), KeyboardButton("17"), KeyboardButton("18"), KeyboardButton("19"), KeyboardButton("20")]
    row5 = [KeyboardButton("21"), KeyboardButton("22"), KeyboardButton("23"), KeyboardButton("24"), KeyboardButton("25")]
    row6 = [KeyboardButton("26"), KeyboardButton("27"), KeyboardButton("28"), KeyboardButton("29"), KeyboardButton("30")]
    row7 = [KeyboardButton("31"), KeyboardButton("❌ Отмена")]
    kb.add(*row1)
    kb.add(*row2)
    kb.add(*row3)
    kb.add(*row4)
    kb.add(*row5)
    kb.add(*row6)
    kb.add(*row7)
    return kb
# ---------------- Helpers & DB access (aiosqlite) ------------
async def init_db():
    """
    Initialize aiosqlite connection, pragmas and tables.
    Called from init_app_for_runtime.
    """
    global db
    db = await aiosqlite.connect(DB_FILE)
    db.row_factory = aiosqlite.Row # type: ignore[attr-defined]
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA foreign_keys=ON;")
    await db.commit()
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
    try:
        if db:
            await db.close()
            logger.info("aiosqlite DB closed")
    except Exception:
        logger.exception("Error while closing DB")
    finally:
        db = None
async def db_execute(query: str, params: tuple = ()):
    if db is None:
        raise RuntimeError("DB not initialized")
    async with (db_lock if db_lock is not None else asyncio.Lock()):
        await db.execute(query, params)
        await db.commit()
async def db_fetchone(query: str, params: tuple = ()):
    if db is None:
        raise RuntimeError("DB not initialized")
    async with (db_lock if db_lock is not None else asyncio.Lock()):
        cur = await db.execute(query, params)
        row = await cur.fetchone()
        await cur.close()
        return row
async def db_fetchall(query: str, params: tuple = ()):
    if db is None:
        raise RuntimeError("DB not initialized")
    async with (db_lock if db_lock is not None else asyncio.Lock()):
        cur = await db.execute(query, params)
        rows = await cur.fetchall()
        await cur.close()
        return rows
# ---------------- DB-backed helpers ------------
async def ensure_user(uid: int, first_name: str = "", username: str = ""):
    global db_lock
    if db_lock is None:
        db_lock = asyncio.Lock()
    
    now = datetime.utcnow().isoformat()
    # Проверяем, существует ли пользователь
    existing = await db_fetchone("SELECT user_id FROM users WHERE user_id = ?", (uid,))
    
    if existing:
        # Обновляем последнюю активность
        await db_execute("UPDATE users SET last_active = ? WHERE user_id = ?", (now, uid))
    else:
        # Создаем нового пользователя
        await db_execute(
            "INSERT INTO users (user_id, first_name, username, created_at, last_active) VALUES (?, ?, ?, ?, ?)",
            (uid, first_name, username, now, now)
        )
async def get_income(uid: int) -> float:
    r = await db_fetchone("SELECT income FROM users WHERE user_id = ?", (uid,))
    return float(r["income"]) if r and r["income"] is not None else 0.0
async def set_income(uid: int, v: float):
    await db_execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (uid,))
    await db_execute("UPDATE users SET income = ? WHERE user_id = ?", (v, uid))
async def get_user_stats(uid: int) -> Dict[str, Any]:
    """Получить статистику пользователя для приветствия"""
    income = await get_income(uid)
    
    # Получаем количество трат за текущий месяц
    now_utc = datetime.utcnow()
    month_start = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    month_end = next_month - timedelta(seconds=1)
    
    expenses_count = await db_fetchone(
        "SELECT COUNT(*) as count FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ?",
        (uid, month_start.isoformat(), month_end.isoformat())
    )
    
    # Получаем общую сумму трат за месяц
    total_spent = await db_fetchone(
        "SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ?",
        (uid, month_start.isoformat(), month_end.isoformat())
    )
    
    return {
        "income": income,
        "expenses_count": expenses_count["count"] if expenses_count else 0,
        "total_spent": total_spent["total"] if total_spent and total_spent["total"] else 0
    }
def format_amount(x):
    try:
        return f"{x:,.0f}".replace(",", " ")
    except Exception:
        return str(x)
def get_limits_from_income(income: float):
    return {cat: income * pct for group in CATEGORIES.values() for cat, pct in group.items()}
async def add_expense(uid, amount, category, ts=None, rec_id=None):
    ts = ts or datetime.utcnow().isoformat()
    await db_execute(
        "INSERT INTO expenses (user_id, amount, category, timestamp, recurring_id) VALUES (?, ?, ?, ?, ?)",
        (uid, amount, category, ts, rec_id)
    )
async def get_expenses(uid, limit=10):
    rows = await db_fetchall(
        "SELECT id, amount, category, timestamp FROM expenses WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?",
        (uid, limit)
    )
    return rows
async def delete_expense(eid):
    await db_execute("DELETE FROM expenses WHERE id = ?", (eid,))
async def check_limits(uid, category, amount):
    """Проверка лимитов с исправленной математикой"""
    income = await get_income(uid)
    if income <= 0:
        return []
        
    limits = get_limits_from_income(income)
    if category not in limits:
        return []

    now_utc = datetime.utcnow()
    month_start = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    month_end = next_month - timedelta(seconds=1)

    # Один запрос для получения всех данных
    query = """
    SELECT 
        SUM(CASE WHEN category = ? THEN amount ELSE 0 END) as cat_total,
        SUM(amount) as total_spent
    FROM expenses 
    WHERE user_id = ? AND timestamp BETWEEN ? AND ?
    """
    
    result = await db_fetchone(query, (category, uid, month_start.isoformat(), month_end.isoformat()))
    
    if not result:
        cat_spent = 0
        total_spent = 0
    else:
        cat_spent = result["cat_total"] or 0
        total_spent = result["total_spent"] or 0
    
    category_limit = limits[category]
    
    warnings = []
    
    # Проверка общего лимита (доход)
    if total_spent + amount > income:
        warnings.append("⚠️ Общий месячный лимит превышен!")
        
    # Проверка лимита по категории
    if cat_spent + amount > category_limit:
        warnings.append(f"⚠️ Лимит по '{category}' превышен!")
    elif cat_spent + amount > 0.9 * category_limit:
        warnings.append(f"⚠️ Ты израсходовал более 90% лимита по '{category}'!")
    
    return warnings
async def format_stats(uid: int) -> str:
    """Статистика с исправленной математикой процентов"""
    income = await get_income(uid)
    limits = get_limits_from_income(income)
    now_utc = datetime.utcnow()
    month_start = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    month_end = next_month - timedelta(seconds=1)
    
    rows = await db_fetchall(
        "SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ? GROUP BY category",
        (uid, month_start.isoformat(), month_end.isoformat())
    )
    
    spent = {r["category"]: (r["total"] or 0) for r in rows}
    
    text = f"💰 Ваш доход: {format_amount(income)} ₽\n\n"
    
    for group, cats in CATEGORIES.items():
        text += f"📂 {group}\n"
        for cat, pct in cats.items():
            lim = limits.get(cat, 0)
            s = spent.get(cat, 0)
            
            # Исправленный расчет процента
            if lim > 0:
                perc = (s / lim) * 100
                perc_text = f"{perc:.0f}%"
            else:
                perc_text = "0%"
                
            text += f"• {cat}: {format_amount(s)} ₽ / {format_amount(lim)} ₽ ({perc_text})\n"
        text += "\n"
    
    # Добавляем общую статистику
    total_spent = sum(spent.values())
    if income > 0:
        total_perc = (total_spent / income) * 100
        text += f"📊 Всего потрачено: {format_amount(total_spent)} ₽ / {format_amount(income)} ₽ ({total_perc:.0f}%)"
    else:
        text += f"📊 Всего потрачено: {format_amount(total_spent)} ₽"
        
    return text
# ---------------- Scheduler ----------------
scheduler = AsyncIOScheduler(timezone=TZ)

def get_last_day_of_month(year: int, month: int) -> int:
    """Получить последний день месяца"""
    if month == 12:
        return 31
    next_month = datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    return last_day.day

async def daily_reminders():
    rows = await db_fetchall("SELECT user_id FROM users WHERE notifications = 1")
    uids = [r["user_id"] for r in rows]
    async def _send(uid):
        try:
            await bot.send_message(uid, "💡 Не забудь добавить траты за сегодня!")
        except Exception as e:
            logger.debug("Failed to send reminder to %s: %s", uid, e)
    tasks = [asyncio.create_task(_send(uid)) for uid in uids]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
async def weekly_report():
    rows = await db_fetchall("SELECT user_id FROM users")
    uids = [r["user_id"] for r in rows]
    async def _send(uid):
        try:
            text = "📊 Еженедельный отчёт:\n\n" + await format_stats(uid)
            await bot.send_message(uid, text)
        except Exception as e:
            logger.debug("Failed to send weekly report to %s: %s", uid, e)
    tasks = [asyncio.create_task(_send(uid)) for uid in uids]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
async def process_recurring():
    """Обработка регулярных расходов с корректировкой дней"""
    now = datetime.utcnow()
    current_day = now.day
    last_day_of_month = get_last_day_of_month(now.year, now.month)
    
    # Получаем все регулярные расходы
    rows = await db_fetchall("SELECT id, user_id, amount, category, day FROM recurring")
    
    async def _handle_row(r):
        try:
            rec_id = r["id"]
            uid = r["user_id"]
            amt = r["amount"]
            cat = r["category"]
            scheduled_day = r["day"]
            
            # Определяем фактический день для выполнения
            actual_day = min(scheduled_day, last_day_of_month)
            
            # Если сегодня подходящий день
            if current_day == actual_day:
                await add_expense(uid, amt, cat, rec_id=rec_id)
                try:
                    day_info = ""
                    if scheduled_day > last_day_of_month:
                        day_info = f" (скорректировано с {scheduled_day}-го на {last_day_of_month}-е)"
                    
                    await bot.send_message(
                        uid, 
                        f"🔁 Добавлен регулярный расход: {format_amount(amt)} ₽ — {cat}{day_info}"
                    )
                except Exception:
                    logger.debug("Failed to notify user %s about recurring expense", uid)
                    
        except Exception as e:
            logger.debug("process_recurring error: %s", e)
    
    tasks = [asyncio.create_task(_handle_row(r)) for r in rows]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

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
def build_limits_table_html(income: float) -> str:
    """
    Build a human-friendly HTML text for limits.
    NOTE: do NOT use <pre> / <code> to avoid Telegram's 'Copy' UI affordance.
    """
    limits = get_limits_from_income(income)
    lines = []
    lines.append(f"<b>Доход:</b> {format_amount(income)} ₽")
    lines.append("")
    lines.append("<b>Рекомендуемые лимиты (процент / сумма):</b>")
    lines.append("")
    for group, cats in CATEGORIES.items():
        lines.append(f"<b>{group}:</b>")
        for cat, pct in cats.items():
            sum_rub = limits[cat]
            pct_str = f"{int(pct*100)}%"
            lines.append(f"• {cat}: {pct_str} — {format_amount(sum_rub)} ₽")
        lines.append("")
    return "\n".join(lines)
# ---------------- Handlers (registered to dp) ----------------
@dp.message_handler(commands=['start'])
async def start(msg: types.Message):
    uid = msg.from_user.id
    first_name = msg.from_user.first_name or ""
    username = msg.from_user.username or ""
    
    await ensure_user(uid, first_name, username)
    
    # Получаем статистику пользователя
    user_stats = await get_user_stats(uid)
    income = user_stats["income"]
    
    if income > 0:
        # Пользователь уже существует с установленным доходом
        welcome = (
            f"<b>С возвращением, {first_name}! 👋</b>\n\n"
            f"Рад снова видеть тебя! Продолжим оптимизировать твои финансы?\n\n"
            f"<b>Твоя текущая статистика:</b>\n"
            f"• Доход: {format_amount(income)} ₽\n"
            f"• Траты в этом месяце: {format_amount(user_stats['total_spent'])} ₽\n"
            f"• Количество операций: {user_stats['expenses_count']}\n\n"
            f"Используй кнопки ниже для управления бюджетом или посмотри статистику детальнее!"
        )
        kb = get_main_keyboard()
        await bot.send_message(msg.chat.id, welcome, reply_markup=kb, parse_mode=types.ParseMode.HTML)
    else:
        # Новый пользователь
        welcome = (
            "<b>Привет! Я — твой финансовый помощник. 🤖💰</b>\n\n"
            "Я помогу тебе отслеживать расходы, планировать бюджет, "
            "настраивать регулярные платежи и вовремя предупреждать о превышениях лимитов.\n\n"
            "<b>Чтобы начать — введи свой ежемесячный доход</b> (например: <b>50 000</b>)\n\n"
            "После ввода дохода я рассчитаю рекомендованные лимиты по категориям и покажу подсказки по кнопкам внизу."
        )
        kb = get_main_keyboard()
        # PENDING: set conversation shim to expect income input
        await set_pending(uid, "income")
        await bot.send_message(msg.chat.id, welcome, reply_markup=kb, parse_mode=types.ParseMode.HTML)

# ---------------- Новые команды отчетов ----------------
@dp.message_handler(commands=['reportweek'])
async def report_week_cmd(msg: types.Message):
    """Отчёт за неделю"""
    now = datetime.utcnow()
    week_start = now - timedelta(days=7)
    
    rows = await db_fetchall(
        "SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp >= ? GROUP BY category",
        (msg.from_user.id, week_start.isoformat())
    )
    
    if not rows:
        await bot.send_message(msg.chat.id, "📊 <b>Отчёт за неделю</b>\n\nНет данных за последние 7 дней.", parse_mode=types.ParseMode.HTML)
        return
    
    total_spent = sum(r["total"] for r in rows if r["total"])
    text = f"📊 <b>Отчёт за неделю</b>\n\n"
    
    for r in rows:
        total = r["total"] if r and r["total"] is not None else 0
        text += f"• {r['category']}: {format_amount(total)} ₽\n"
    
    text += f"\n<b>Итого:</b> {format_amount(total_spent)} ₽"
    await bot.send_message(msg.chat.id, text, parse_mode=types.ParseMode.HTML)

@dp.message_handler(commands=['reportmonth'])
async def report_month_cmd(msg: types.Message):
    """Отчёт за текущий месяц"""
    now_utc = datetime.utcnow()
    month_start = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
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
    text = f"📊 <b>Отчёт за {month_start.strftime('%B')}</b>\n\n"
    
    for r in rows:
        total = r["total"] if r and r["total"] is not None else 0
        text += f"• {r['category']}: {format_amount(total)} ₽\n"
    
    text += f"\n<b>Итого:</b> {format_amount(total_spent)} ₽"
    await bot.send_message(msg.chat.id, text, parse_mode=types.ParseMode.HTML)

# --- Generic text handler that first looks at pending_actions (PENDING) ---
@dp.message_handler(content_types=['text'])
async def generic_text_handler(msg: types.Message):
    uid = msg.from_user.id
    text = (msg.text or "").strip()
    
    # Обработка отмены для всех pending действий
    if text == "❌ Отмена":
        await pop_pending(uid)
        await bot.send_message(
            uid, 
            "❌ Действие отменено. Используйте кнопки ниже для продолжения.", 
            reply_markup=get_main_keyboard()
        )
        return
    
    # Обработка специальных кнопок цифровой клавиатуры
    if text in ["✅ Готово", "🗑️ Очистить"]:
        pending = await get_pending(uid)
        if not pending:
            await bot.send_message(uid, "Нечего подтверждать. Используйте кнопки ниже.", reply_markup=get_main_keyboard())
            return
            
        ptype = pending.get("type")
        pdata = pending.get("data", {})
        current_input = pdata.get("current_input", "")
        
        if text == "🗑️ Очистить":
            pdata["current_input"] = ""
            async with pending_lock:
                pending_actions[uid]["data"] = pdata
            await bot.send_message(
                uid, 
                "🗑️ Ввод очищен. Введите сумму:", 
                reply_markup=get_digits_keyboard()
            )
            return
            
        elif text == "✅ Готово":
            if not current_input:
                await bot.send_message(
                    uid, 
                    "❌ Сначала введите сумму!", 
                    reply_markup=get_digits_keyboard()
                )
                return
                
            try:
                amount = float(current_input)
                if ptype == "expense_amount":
                    pdata['amount'] = amount
                    async with pending_lock:
                        pending_actions[uid]['data'] = pdata
                        pending_actions[uid]['type'] = "expense_choose_category"
                    kb = InlineKeyboardMarkup(row_width=2)
                    for cat in ALL_CATEGORIES:
                        kb.insert(InlineKeyboardButton(cat, callback_data=f"cat_{cat}"))
                    await bot.send_message(uid, f"Сумма: {format_amount(amount)} ₽\n\nВыбери категорию:", reply_markup=kb)
                    
                elif ptype == "recurring_amount":
                    pdata['amount'] = amount
                    async with pending_lock:
                        pending_actions[uid]['data'] = pdata
                        pending_actions[uid]['type'] = "recurring_choose_category"
                    kb = InlineKeyboardMarkup(row_width=2)
                    for cat in ALL_CATEGORIES:
                        kb.insert(InlineKeyboardButton(cat, callback_data=f"rec_{cat}"))
                    await bot.send_message(uid, f"Сумма: {format_amount(amount)} ₽\n\nВыбери категорию:", reply_markup=kb)
                    
            except ValueError:
                await bot.send_message(uid, "❌ Ошибка преобразования суммы. Попробуйте снова.", reply_markup=get_digits_keyboard())
            return
    
    if text.startswith("/"):
        return # let command handlers process
        
    pending = await get_pending(uid)
    if pending:
        ptype = pending.get("type")
        pdata = pending.get("data", {})
        logger.info("PENDING: processing %s input from %s -> %s", ptype, uid, text[:50])
        
        # Обработка цифрового ввода для сумм
        if ptype in ["expense_amount", "recurring_amount"] and text.isdigit():
            current_input = pdata.get("current_input", "")
            current_input += text
            pdata["current_input"] = current_input
            async with pending_lock:
                pending_actions[uid]["data"] = pdata
                
            display_amount = format_amount(float(current_input)) if current_input else "0"
            await bot.send_message(
                uid, 
                f"💸 Вводимая сумма: {display_amount} ₽\n\nПродолжайте ввод цифр или нажмите '✅ Готово'", 
                reply_markup=get_digits_keyboard()
            )
            return
            
        elif ptype == "income":
            try:
                income = float(text.replace(" ", "").replace(",", "."))
                await set_income(uid, income)
                await pop_pending(uid)
                table_html = build_limits_table_html(income)
                buttons_expl = (
                    "<b>Кнопки:</b>\n"
                    "➕ <b>Добавить трату</b> — добавьте расход вручную: введите сумму и выберите категорию.\n\n"
                    "📜 <b>История</b> — просмотр последних трат с категориями, временем и кнопкой удаления.\n\n"
                    "📊 <b>Моя статистика</b> — текущие расходы по категориям и сравнение с лимитами.\n\n"
                    "ℹ️ <b>Помощь</b> — список доступных команд и быстрых подсказок."
                )
                await bot.send_message(uid, table_html + "\n\n" + buttons_expl, parse_mode=types.ParseMode.HTML, reply_markup=get_main_keyboard())
            except Exception:
                await bot.send_message(uid, "❌ Неверный формат дохода. Введите число, например: 50 000.", reply_markup=get_cancel_keyboard())
            return
            
        elif ptype == "recurring_day":
            try:
                day = int(text)
                if not (1 <= day <= 31):
                    raise ValueError
                data = pdata
                
                # Сохраняем регулярный расход
                await db_execute(
                    "INSERT INTO recurring (user_id, amount, category, day) VALUES (?, ?, ?, ?)",
                    (uid, data["amount"], data["category"], day)
                )
                
                # НЕМЕДЛЕННО добавляем расход в текущие траты
                await add_expense(uid, data["amount"], data["category"])
                
                await pop_pending(uid)
                
                response_text = (
                    f"✅ <b>Регулярный расход добавлен!</b>\n\n"
                    f"• Сумма: {format_amount(data['amount'])} ₽\n"
                    f"• Категория: {data['category']}\n"
                    f"• Дата: каждое {day}-е число\n"
                    f"• <i>Расход также добавлен в текущие траты</i>"
                )
                await bot.send_message(uid, response_text, parse_mode=types.ParseMode.HTML, reply_markup=get_main_keyboard())
                
            except Exception:
                await bot.send_message(
                    uid, 
                    "❌ Укажи число от 1 до 31. Если выбранного дня нет в месяце, расход будет добавлен в последний день месяца.",
                    reply_markup=get_days_keyboard()
                )
            return
            
        else:
            await pop_pending(uid)
            logger.warning("PENDING: unknown type %s for user %s - cleared", ptype, uid)
            await bot.send_message(uid, "Произошла ошибка, пожалуйста повторите действие.", reply_markup=get_main_keyboard())
            return
            
    # If no pending action, handle main keyboard texts
    if text == "➕ Добавить трату":
        await set_pending(uid, "expense_amount", {"current_input": ""})
        await bot.send_message(
            uid, 
            "💸 Введите сумму траты с помощью цифровой клавиатуры:\n\nНажимайте цифры, затем '✅ Готово'", 
            reply_markup=get_digits_keyboard()
        )
        return
        
    if text == "📜 История":
        await history(msg)
        return
        
    if text == "📊 Моя статистика":
        await stats(msg)
        return
        
    if text == "ℹ️ Помощь":
        await help_cmd(msg)
        return
        
    await bot.send_message(uid, "Не понял. Используйте кнопки или /start, /help.", reply_markup=get_main_keyboard())
    
# ---------------- Callback handlers (no strict FSM dependency) ----------------
@dp.callback_query_handler(lambda c: c.data and c.data.startswith('cat_'))
async def expense_category(cb: types.CallbackQuery):
    cat = cb.data[4:]
    uid = cb.from_user.id
    pending = await get_pending(uid)
    if pending and pending.get("type") in ("expense_choose_category", "expense_amount"):
        data = pending.get("data", {})
        amount = data.get("amount")
        if amount is None:
            await cb.answer("Сначала укажите сумму траты.")
            try:
                await cb.message.edit_text("💸 Введите сумму траты:")
            except Exception:
                pass
            async with pending_lock:
                pending_actions[uid] = {"type": "expense_amount", "data": {"current_input": ""}}
            return
        await add_expense(uid, amount, cat)
        await pop_pending(uid)
        try:
            await cb.message.edit_text(f"✅ Добавлено: {format_amount(amount)} ₽ — {cat}")
        except Exception:
            await bot.send_message(uid, f"✅ Добавлено: {format_amount(amount)} ₽ — {cat}", reply_markup=get_main_keyboard())
        warnings = await check_limits(uid, cat, amount)
        if warnings:
            await bot.send_message(uid, "\n".join(warnings), reply_markup=get_main_keyboard())
        else:
            # Если нет предупреждений, все равно показываем основную клавиатуру
            await bot.send_message(uid, "Используйте кнопки ниже для продолжения:", reply_markup=get_main_keyboard())
        return
    else:
        await cb.answer("Сначала укажите сумму траты.")
        try:
            await cb.message.edit_text("💸 Введите сумму траты:")
        except Exception:
            pass
        await set_pending(uid, "expense_amount", {"current_input": ""})
        return
        
@dp.callback_query_handler(lambda c: c.data and c.data.startswith('rec_'))
async def recurring_category(cb: types.CallbackQuery):
    cat = cb.data[4:]
    uid = cb.from_user.id
    pending = await get_pending(uid)
    if pending and pending.get("type") in ("recurring_choose_category", "recurring_amount"):
        data = pending.get("data", {})
        amount = data.get("amount")
        if amount is None:
            await cb.answer("Сначала укажите сумму регулярного расхода.")
            try:
                await cb.message.edit_text("Введите сумму регулярного расхода:")
            except Exception:
                pass
            async with pending_lock:
                pending_actions[uid] = {"type": "recurring_amount", "data": {"current_input": ""}}
            return
        async with pending_lock:
            pending_actions[uid] = {"type": "recurring_day", "data": {"amount": amount, "category": cat}}
        try:
            await cb.message.edit_text("Укажи день месяца (1–31):")
        except Exception:
            await bot.send_message(uid, "Укажи день месяца (1–31):", reply_markup=get_days_keyboard())
        return
    else:
        await cb.answer("Сначала укажите сумму регулярного расхода.")
        try:
            await cb.message.edit_text("Введите сумму регулярного расхода:")
        except Exception:
            pass
        await set_pending(uid, "recurring_amount", {"current_input": ""})
        return
        
# ---------------- Other handlers ----------------
@dp.message_handler(lambda m: m.text == "📜 История")
async def history(msg: types.Message):
    exps = await get_expenses(msg.from_user.id)
    if not exps:
        await bot.send_message(msg.chat.id, "Пока нет трат 💰")
        return
    for e in exps:
        ts = e['timestamp']
        try:
            dt = datetime.fromisoformat(ts).strftime('%d.%m %H:%M')
        except Exception:
            dt = ts
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Удалить", callback_data=f"del_{e['id']}"))
        await bot.send_message(msg.chat.id, f"{dt} | {e['amount']:,.0f} ₽ | {e['category']}", reply_markup=kb)
        
@dp.callback_query_handler(lambda c: c.data and c.data.startswith('del_'))
async def delete_expense_cb(cb: types.CallbackQuery):
    eid = int(cb.data[4:])
    await delete_expense(eid)
    await cb.answer("Удалено")
    try:
        await cb.message.delete()
    except Exception:
        pass
        
@dp.message_handler(lambda m: m.text == "📊 Моя статистика")
async def stats(msg: types.Message):
    text = await format_stats(msg.from_user.id)
    await bot.send_message(msg.chat.id, text)

@dp.message_handler(lambda m: m.text == "ℹ️ Помощь")
async def help_cmd(msg: types.Message):
    help_text = (
        "📋 <b>Доступные команды:</b>\n\n"
        "➕ <b>Добавить трату</b> - быстро добавить расход через кнопки\n\n"
        "📊 <b>Моя статистика</b> - текущее состояние бюджета\n\n"
        "📜 <b>История</b> - последние траты с возможностью удаления\n\n"
        "<b>Быстрые команды:</b>\n"
        "/reportweek - отчёт за последние 7 дней\n"
        "/reportmonth - отчёт за текущий месяц\n"
        "/add_recurring - добавить регулярный расход\n\n"
        "Просто используй кнопки ниже для основных действий! 💰"
    )
    await bot.send_message(msg.chat.id, help_text, parse_mode=types.ParseMode.HTML)

@dp.message_handler(commands=['add_recurring'])
async def add_recurring(msg: types.Message):
    """Добавление регулярного расхода с выбором дня 1-31"""
    uid = msg.from_user.id
    await set_pending(uid, "recurring_amount", {"current_input": ""})
    await bot.send_message(
        msg.chat.id, 
        "💸 <b>Добавление регулярного расхода</b>\n\n"
        "Введите сумму регулярного расхода с помощью цифровой клавиатуры:\n\nНажимайте цифры, затем '✅ Готово'",
        parse_mode=types.ParseMode.HTML,
        reply_markup=get_digits_keyboard()
    )

# ---------------- Init helper to be called from main.py on startup ------------
async def init_app_for_runtime(app):
    global db_lock
    if db_lock is None:
        db_lock = asyncio.Lock()
    await init_db()
    _add_scheduler_jobs_once()
    try:
        scheduler.start()
        logger.info("Scheduler started (bot_app)")
    except Exception:
        logger.exception("Failed to start scheduler (bot_app)")
    try:
        sess = await bot.get_session()
        app['bot_session'] = sess
    except Exception:
        logger.debug("bot.get_session() failed during bot_app init (may be fine)")
        
# Exported names for main.py convenience
__all__ = ("bot", "dp", "scheduler", "init_app_for_runtime", "get_main_keyboard", "format_stats", "close_db")
