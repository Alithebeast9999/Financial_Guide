"""
Robust, fixed bot_app.py for the financial assistant bot.
Key points:
- Uses aiosqlite for async DB access
- Dispatcher + FSM handlers implemented reliably for webhook+worker setups
- Safe HTML usage (explicit parse_mode where needed)
- Recurring expenses are immediately added to expenses table and can be deleted
- Help text uses reportweek/reportmonth (no cancel mention)
- Allows day 1..31 for recurring; maps to last day of month when necessary
- Exports init_app_for_runtime and close_db as expected by main.py
"""
import os
import logging
import asyncio
from datetime import datetime, timedelta, date
import calendar
import html as html_lib
import aiosqlite
import pytz

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ParseMode

logger = logging.getLogger(__name__)

# Config
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

# timezone
TZ = pytz.timezone("Europe/Moscow")

# Bot/Dispatcher: use parse_mode=None globally; use explicit parse_mode where needed
BotClass = Bot  # exportable class alias for main.py compatibility
bot = BotClass(token=BOT_TOKEN, timeout=30, parse_mode=None)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# aiosqlite DB
DB_FILE = os.environ.get("DB_FILE", "bot.db")
_db = None  # aiosqlite.Connection
_db_lock = None  # asyncio.Lock

# Scheduler
scheduler = AsyncIOScheduler(timezone=TZ)

# Categories and UI
CATEGORIES = {
    "НАДО": {"Аренда жилья": 0.35, "Продуктовая корзина": 0.15, "Комм. услуги": 0.05, "Связь": 0.03, "Транспорт": 0.05, "Личный уход": 0.02, "Медицина": 0.08},
    "МОГУ": {"Инвестиции": 0.05, "Подушка безопасности": 0.05},
    "ХОЧУ": {"Развлечения": 0.07, "Отдых - путешествия": 0.05, "Покупки": 0.05},
}
ALL_CATEGORIES = [c for g in CATEGORIES.values() for c in g]
MAIN_BUTTONS = {"➕ Добавить трату", "📜 История", "📊 Моя статистика", "ℹ️ Помощь"}

# FSM
class IncomeState(StatesGroup):
    income = State()

class ExpenseState(StatesGroup):
    amount = State()
    category = State()

class RecurringState(StatesGroup):
    amount = State()
    category = State()
    day = State()

# ---------------- DB helpers ----------------
async def init_db_connection():
    global _db, _db_lock
    if _db is None:
        _db = await aiosqlite.connect(DB_FILE)
        _db.row_factory = aiosqlite.Row
    if _db_lock is None:
        _db_lock = asyncio.Lock()

async def close_db():
    global _db
    try:
        if _db:
            await _db.close()
            logger.info('aiosqlite DB closed')
    finally:
        _db = None

async def db_execute(query: str, params: tuple = ()):  # for INSERT/UPDATE/DELETE
    if _db is None or _db_lock is None:
        raise RuntimeError("DB not initialized")
    async with _db_lock:
        cur = await _db.execute(query, params)
        await _db.commit()
        return cur

async def db_fetchone(query: str, params: tuple = ()):  # returns Row or None
    if _db is None or _db_lock is None:
        raise RuntimeError("DB not initialized")
    async with _db_lock:
        cur = await _db.execute(query, params)
        row = await cur.fetchone()
        await cur.close()
        return row

async def db_fetchall(query: str, params: tuple = ()):  # returns list
    if _db is None or _db_lock is None:
        raise RuntimeError("DB not initialized")
    async with _db_lock:
        cur = await _db.execute(query, params)
        rows = await cur.fetchall()
        await cur.close()
        return rows

# ---------------- Initialization ----------------
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

async def init_app_for_runtime(app=None):
    """Called from main.py to initialize DB lock, scheduler and ensure tables."""
    await init_db_connection()
    _add_scheduler_jobs_once()
    try:
        scheduler.start()
        logger.info("Scheduler started (bot_app)")
    except Exception:
        logger.exception("Failed to start scheduler (bot_app)")

    # ensure tables
    try:
        async with _db_lock:
            await _db.execute("""CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, income REAL DEFAULT 0, notifications INTEGER DEFAULT 1)""")
            await _db.execute("""CREATE TABLE IF NOT EXISTS expenses (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, timestamp TEXT, recurring_id INTEGER)""")
            await _db.execute("""CREATE TABLE IF NOT EXISTS recurring (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, day INTEGER)""")
            await _db.commit()
    except Exception:
        logger.exception("DB ensure tables failed (ignored)")

# ---------------- Helpers ----------------
async def ensure_user(uid: int):
    await db_execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (uid,))

async def get_income(uid: int) -> float:
    r = await db_fetchone("SELECT income FROM users WHERE user_id = ?", (uid,))
    return float(r["income"]) if r and r["income"] is not None else 0.0

async def set_income(uid: int, v: float):
    await db_execute("INSERT OR REPLACE INTO users (user_id, income) VALUES (?, ?)", (uid, v))

def format_amount(x):
    try:
        return f"{x:,.0f}".replace(",", " ")
    except Exception:
        return str(x)

def get_limits_from_income(income: float):
    return {cat: income * pct for group in CATEGORIES.values() for cat, pct in group.items()}

async def add_expense(uid, amount, category, ts=None, rec_id=None):
    ts = ts or datetime.now(TZ)
    await db_execute("INSERT INTO expenses (user_id, amount, category, timestamp, recurring_id) VALUES (?, ?, ?, ?, ?)",
                     (uid, amount, category, ts.isoformat(), rec_id))

async def get_expenses(uid, limit=10):
    rows = await db_fetchall("SELECT id, amount, category, timestamp, recurring_id FROM expenses WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?", (uid, limit))
    return rows

async def delete_expense(eid):
    await db_execute("DELETE FROM expenses WHERE id = ?", (eid,))

async def delete_recurring(rid):
    # remove from recurring table and associated future recurring entries aren't created until process_recurring runs
    await db_execute("DELETE FROM recurring WHERE id = ?", (rid,))

async def check_limits(uid, category, amount):
    limits = get_limits_from_income(await get_income(uid))
    if category not in limits:
        return []
    income = await get_income(uid)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    r = await db_fetchone("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ?", (uid, month_start.isoformat(), month_end.isoformat()))
    total_spent = r["total"] if r and r["total"] is not None else 0
    r = await db_fetchone("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND category = ? AND timestamp BETWEEN ? AND ?", (uid, category, month_start.isoformat(), month_end.isoformat()))
    cat_spent = r["total"] if r and r["total"] is not None else 0
    msgs = []
    if total_spent + amount > income:
        msgs.append("⚠️ Общий месячный лимит превышен!")
    if cat_spent + amount > limits[category]:
        msgs.append(f"⚠️ Лимит по '{category}' превышен!")
    elif cat_spent + amount > 0.9 * limits[category]:
        msgs.append(f"⚠️ Ты израсходовал более 90% лимита по '{category}'!")
    return msgs

async def format_stats(uid: int) -> str:
    income = await get_income(uid)
    limits = get_limits_from_income(income)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    rows = await db_fetchall("SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ? GROUP BY category", (uid, month_start.isoformat(), month_end.isoformat()))
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

# ---------------- Scheduler job functions ----------------
async def daily_reminders():
    rows = await db_fetchall("SELECT user_id FROM users WHERE notifications = 1", ())
    for r in rows:
        uid = r[0]
        try:
            await bot.send_message(uid, "💡 Не забудь добавить траты за сегодня!")
        except Exception:
            logger.debug("Failed to send reminder to %s", uid)

async def weekly_report():
    rows = await db_fetchall("SELECT user_id FROM users", ())
    for r in rows:
        uid = r[0]
        try:
            await bot.send_message(uid, "📊 Еженедельный отчёт:\n\n" + await format_stats(uid))
        except Exception:
            logger.debug("Failed to send weekly report to %s", uid)

async def process_recurring():
    """Run once a day: add recurring expenses for today (matching day).
    If recurring day is greater than days in current month, it will be applied
    to the last day of the month automatically when that date arrives.
    """
    today = datetime.now(TZ).day
    rows = await db_fetchall("SELECT id, user_id, amount, category, day FROM recurring", ())
    for r in rows:
        rec_id, uid, amt, cat, rday = r
        # Determine whether this recurring should trigger today
        # If recurring day is > days_in_month, trigger on the last day of the month
        days_in_month = calendar.monthrange(datetime.now(TZ).year, datetime.now(TZ).month)[1]
        trigger_day = min(rday, days_in_month)
        if trigger_day == today:
            await add_expense(uid, amt, cat, rec_id=rec_id)
            try:
                await bot.send_message(uid, f"🔁 Добавлен регулярный расход: {format_amount(amt)} ₽ — {html_lib.escape(cat)}")
            except Exception:
                logger.debug("Failed to send recurring notification to %s", uid)

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

# ---------------- Handlers ----------------
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
    # explicit HTML parse_mode
    await msg.reply(welcome, reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.message_handler(commands=['cancel'], state="*")
async def cmd_cancel(msg: types.Message, state: FSMContext):
    cur = await state.get_state()
    if cur is None:
        await msg.reply("Нечего отменять.")
        return
    await state.finish()
    await msg.reply("Действие отменено. Можешь использовать кнопки ниже.", reply_markup=get_main_keyboard())

@dp.message_handler(state=IncomeState.income)
async def set_income_handler(msg: types.Message, state: FSMContext):
    text = (msg.text or "").strip()
    if text.startswith("/"):
        await state.finish()
        if text.startswith("/start"):
            await start(msg)
        else:
            await msg.reply("Команда выполнена. Если вы хотели ввести доход — введите число.")
        return
    if text in MAIN_BUTTONS:
        await state.finish()
        if text == "📜 История":
            await history(msg)
        elif text == "📊 Моя статистика":
            await stats(msg)
        elif text == "ℹ️ Помощь":
            await help_cmd(msg)
        elif text == "➕ Добавить трату":
            await add_expense_cmd(msg)
        return
    try:
        income = float(text.replace(" ", "").replace(",", "."))
    except Exception:
        await msg.reply("❌ Неверный формат дохода. Введите число, например: 50 000 (или нажмите /cancel).")
        return
    await set_income(msg.from_user.id, income)
    await state.finish()
    table_html = build_limits_table_html(income)
    buttons_expl = (
        "<b>Кнопки:</b>\n"
        "➕ <b>Добавить трату</b> — добавьте расход вручную: введите сумму и выберите категорию.\n\n"
        "📜 <b>История</b> — просмотр последних трат с категориями, временем и кнопкой удаления.\n\n"
        "📊 <b>Моя статистика</b> — текущие расходы по категориям и сравнение с лимитами.\n\n"
        "ℹ️ <b>Помощь</b> — список доступных команд и быстрых подсказок."
    )
    full_msg = table_html + "\n\n" + buttons_expl
    kb = get_main_keyboard()
    # send as HTML
    await msg.reply(full_msg, reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.message_handler(lambda m: m.text == "➕ Добавить трату")
async def add_expense_cmd(msg: types.Message):
    await msg.reply("💸 Введи сумму траты (например: 450): (или /cancel чтобы отменить)")
    await ExpenseState.amount.set()

@dp.message_handler(state=ExpenseState.amount)
async def expense_amount(msg: types.Message, state: FSMContext):
    text = msg.text or ""
    if text.startswith("/"):
        await state.finish()
        if text.startswith("/start"):
            await start(msg)
        else:
            await msg.reply("Команда зарегистрирована. Если вы хотели ввести сумму — попробуйте снова.", reply_markup=get_main_keyboard())
        return
    if text in MAIN_BUTTONS:
        await state.finish()
        if text == "📜 История":
            await history(msg)
        elif text == "📊 Моя статистика":
            await stats(msg)
        elif text == "ℹ️ Помощь":
            await help_cmd(msg)
        elif text == "➕ Добавить трату":
            await add_expense_cmd(msg)
        return
    try:
        amount = float(text.replace(" ", "").replace(",", "."))
        await state.update_data(amount=amount)
        kb = InlineKeyboardMarkup(row_width=2)
        for cat in ALL_CATEGORIES:
            kb.insert(InlineKeyboardButton(cat, callback_data=f"cat_{cat}"))
        await msg.reply("Выбери категорию:", reply_markup=kb)
        await ExpenseState.category.set()
    except Exception:
        await msg.reply("❌ Неверная сумма. Введите число, например: 450. Или нажмите /cancel, чтобы отменить.")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('cat_'))
async def expense_category(cb: types.CallbackQuery):
    """Handle category selection. Works even when FSM matching may be unreliable in webhook setups.
    We try to fetch FSM data for the (chat, user) and complete the flow.
    """
    await cb.answer()  # quick feedback
    cat = cb.data[4:]
    # Try to get FSM data
    try:
        state = dp.current_state(chat=cb.message.chat.id if cb.message else None, user=cb.from_user.id)
        data = await state.get_data()
    except Exception:
        state = None
        data = {}

    amount = data.get('amount')
    uid = cb.from_user.id
    if amount is None:
        # nothing to finish
        try:
            await cb.answer("Не найдена сумма. Нажми ➕ Добавить трату и попробуй снова.", show_alert=True)
        except Exception:
            pass
        return

    try:
        warnings = await check_limits(uid, cat, amount)
        await add_expense(uid, amount, cat)
        safe_cat = html_lib.escape(cat)
        try:
            await cb.message.edit_text(f"✅ Добавлено: {format_amount(amount)} ₽ — {safe_cat}")
        except Exception:
            await bot.send_message(uid, f"✅ Добавлено: {format_amount(amount)} ₽ — {safe_cat}")
        if warnings:
            await bot.send_message(uid, "\n".join(warnings))
    except Exception:
        logger.exception("Failed to add expense for user %s: %s %s", uid, amount, cat)
        try:
            await cb.answer("Ошибка при сохранении траты.", show_alert=True)
        except Exception:
            pass
    finally:
        if state:
            try:
                await state.finish()
            except Exception:
                pass

@dp.message_handler(lambda m: m.text == "📜 История")
async def history(msg: types.Message):
    exps = await get_expenses(msg.from_user.id, limit=20)
    if not exps:
        await msg.reply("Пока нет трат 💰")
        return
    # Show recent expenses and mark those that are from recurring
    for e in exps:
        ts = e['timestamp']
        try:
            dt = datetime.fromisoformat(ts).strftime('%d.%m %H:%M')
        except Exception:
            dt = ts
        rb = []
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("❌ Удалить", callback_data=f"del_{e['id']}"))
        if e.get('recurring_id'):
            kb.add(InlineKeyboardButton("❌ Удалить регулярный", callback_data=f"delrec_{e['recurring_id']}"))
        await msg.reply(f"{dt} | {e['amount']:,.0f} ₽ | {e['category']}", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('del_'))
async def delete_expense_cb(cb: types.CallbackQuery):
    eid = int(cb.data[4:])
    await delete_expense(eid)
    await cb.answer("Удалено")
    try:
        await cb.message.delete()
    except Exception:
        pass

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('delrec_'))
async def delete_recurring_cb(cb: types.CallbackQuery):
    rid = int(cb.data[7:])
    await delete_recurring(rid)
    await cb.answer("Регулярный расход удалён")
    try:
        await cb.message.delete()
    except Exception:
        pass

@dp.message_handler(lambda m: m.text == "📊 Моя статистика")
async def stats(msg: types.Message):
    await msg.reply(await format_stats(msg.from_user.id))

# Help command - plain text, uses reportweek/reportmonth
@dp.message_handler(lambda m: m.text == "ℹ️ Помощь")
async def help_cmd(msg: types.Message):
    help_text = (
        "/reportweek — отчёт за неделю\n"
        "/reportmonth — отчёт за месяц\n"
        "/add_recurring — добавить регулярный расход\n"
        "/notify — включить/выключить уведомления"
    )
    await msg.reply(help_text)

@dp.message_handler(commands=['notify'])
async def toggle_notify(msg: types.Message):
    uid = msg.from_user.id
    r = await db_fetchone("SELECT notifications FROM users WHERE user_id = ?", (uid,))
    current = bool(r['notifications']) if r else True
    new_val = 0 if current else 1
    await db_execute("UPDATE users SET notifications = ? WHERE user_id = ?", (new_val, uid))
    await msg.reply("🔔 Уведомления включены" if new_val else "🔕 Уведомления отключены")

@dp.message_handler(commands=['add_recurring'])
async def add_recurring(msg: types.Message):
    await msg.reply("Введи сумму регулярного расхода (или /cancel):")
    await RecurringState.amount.set()

@dp.message_handler(state=RecurringState.amount)
async def recurring_amount(msg: types.Message, state: FSMContext):
    text = msg.text or ""
    if text.startswith("/"):
        await state.finish()
        if text.startswith("/start"):
            await start(msg)
        else:
            await msg.reply("Команда выполнена. Если вы хотели ввести сумму — введите число.")
        return
    if text in MAIN_BUTTONS:
        await state.finish()
        if text == "📜 История":
            await history(msg)
        elif text == "📊 Моя статистика":
            await stats(msg)
        elif text == "ℹ️ Помощь":
            await help_cmd(msg)
        elif text == "➕ Добавить трату":
            await add_expense_cmd(msg)
        return
    try:
        amt = float(text.replace(" ", "").replace(",", "."))
        await state.update_data(amount=amt)
        kb = InlineKeyboardMarkup(row_width=2)
        for cat in ALL_CATEGORIES:
            kb.insert(InlineKeyboardButton(cat, callback_data=f"rec_{cat}"))
        await msg.reply("Выбери категорию:", reply_markup=kb)
        await RecurringState.category.set()
    except Exception:
        await msg.reply("❌ Неверная сумма. Введите число или /cancel.")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('rec_'))
async def recurring_category(cb: types.CallbackQuery):
    await cb.answer()
    cat = cb.data[4:]
    # Try to get FSM state
    try:
        state = dp.current_state(chat=cb.message.chat.id if cb.message else None, user=cb.from_user.id)
        data = await state.get_data()
    except Exception:
        state = None
        data = {}
    if 'amount' not in data:
        try:
            await cb.answer("Не найдена сумма. Повторите команду /add_recurring.", show_alert=True)
        except Exception:
            pass
        return
    # store category and ask for day
    await state.update_data(category=cat)
    try:
        await cb.message.edit_text("Укажи день месяца (1–31):")
    except Exception:
        try:
            await bot.send_message(cb.from_user.id, "Укажи день месяца (1–31):")
        except Exception:
            pass
    await RecurringState.day.set()

@dp.message_handler(state=RecurringState.day)
async def recurring_day(msg: types.Message, state: FSMContext):
    text = msg.text or ""
    if text.startswith("/"):
        await state.finish()
        if text.startswith("/start"):
            await start(msg)
        else:
            await msg.reply("Команда выполнена. Если вы хотели указать день — введите число.")
        return
    if text in MAIN_BUTTONS:
        await state.finish()
        if text == "📜 История":
            await history(msg)
        elif text == "📊 Моя статистика":
            await stats(msg)
        elif text == "ℹ️ Помощь":
            await help_cmd(msg)
        elif text == "➕ Добавить трату":
            await add_expense_cmd(msg)
        return
    try:
        day = int(text)
        if not (1 <= day <= 31):
            raise ValueError
        data = await state.get_data()
        # insert recurring
        cur = await db_execute("INSERT INTO recurring (user_id, amount, category, day) VALUES (?, ?, ?, ?)",
                               (msg.from_user.id, data["amount"], data["category"], day))
        # also add an immediate expense so it appears in stats/history
        await add_expense(msg.from_user.id, data["amount"], data["category"], rec_id=cur.lastrowid)
        await msg.reply(f"🔁 Регулярный расход сохранён: {format_amount(data['amount'])} ₽ — {data['category']} (каждое {day}-е число)")
        await state.finish()
    except Exception:
        await msg.reply("❌ Укажи число от 1 до 31 или /cancel")

@dp.message_handler(commands=['reportweek', 'reportmonth'])
async def report_cmd(msg: types.Message):
    cmd = msg.text.split()[0].lstrip('/')
    args = 'week' if cmd == 'reportweek' else 'month'
    now = datetime.now(TZ)
    start = now - timedelta(days=7) if args == 'week' else now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    rows = await db_fetchall("SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp >= ? GROUP BY category", (msg.from_user.id, start.isoformat()))
    if not rows:
        await msg.reply("Нет данных за выбранный период.")
        return
    text = f"📊 Отчёт за {'неделю' if args == 'week' else 'месяц'}:\n\n"
    for r in rows:
        text += f"{r['category']}: {r['total']:,.0f} ₽\n"
    await msg.reply(text)

# Generic text handler — unified entry point and safety router.
@dp.message_handler()
async def generic_text_handler(msg: types.Message):
    text = (msg.text or "").strip()
    # 0) If it's a bot command — don't handle here
    if text.startswith("/"):
        return
    # 1) Try to determine the FSM state for (chat, user)
    try:
        try:
            state = await dp.current_state(chat=msg.chat.id, user=msg.from_user.id).get_state()
        except Exception:
            try:
                state = await dp.current_state(user=msg.from_user.id).get_state()
            except Exception:
                state = None
    except Exception:
        state = None
    # 2) If user has a state -> MANUAL ROUTING (safety net)
    if state:
        logger.info("User %s has FSM state: %s — manual routing attempt", msg.from_user.id, state)
        state_obj = dp.current_state(chat=msg.chat.id, user=msg.from_user.id)
        s = (state or "").lower()
        if "income" in s:
            try:
                await set_income_handler(msg, state_obj)
            except Exception:
                logger.exception("Manual routing to set_income_handler failed for %s", msg.from_user.id)
            return
        if "expense" in s and "amount" in s:
            try:
                await expense_amount(msg, state_obj)
            except Exception:
                logger.exception("Manual routing to expense_amount failed for %s", msg.from_user.id)
            return
        if "recurring" in s:
            if "amount" in s:
                try:
                    await recurring_amount(msg, state_obj)
                except Exception:
                    logger.exception("Manual routing to recurring_amount failed for %s", msg.from_user.id)
                return
            if "day" in s:
                try:
                    await recurring_day(msg, state_obj)
                except Exception:
                    logger.exception("Manual routing to recurring_day failed for %s", msg.from_user.id)
                return
            if "category" in s:
                # waiting for callback
                return
        return
    # 3) No state -> normal UI routing
    if text in MAIN_BUTTONS:
        if text == "➕ Добавить трату":
            await add_expense_cmd(msg)
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
    # Quick-expense heuristic
    parts = text.split()
    if len(parts) >= 2 and parts[-1].replace(',', '').replace('.', '').isdigit():
        amount_raw = parts[-1]
        try:
            amount = float(amount_raw.replace(' ', '').replace(',', '.'))
            category_guess = ' '.join(parts[:-1])
            cat = None
            for c in ALL_CATEGORIES:
                if c.lower() in category_guess.lower():
                    cat = c
                    break
            if not cat:
                cat = 'Другое'
            await add_expense(msg.from_user.id, amount, cat)
            await msg.reply(f"✅ Добавлено: {format_amount(amount)} ₽ — {cat}")
            return
        except Exception:
            logger.exception("Failed to parse quick-expense message: %s", text)
    await msg.reply("Я не понял. Используй кнопки или нажми ℹ️ Помощь.")

__all__ = ("bot", "dp", "scheduler", "init_app_for_runtime", "close_db", "get_main_keyboard", "format_stats", "BotClass")
