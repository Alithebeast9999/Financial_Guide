# bot_app.py
import os
import logging
import asyncio
from datetime import datetime, timedelta
import pytz
from typing import Dict, Any, Optional
import json
import time
import re

import aiosqlite

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger(__name__)

# Config
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

TZ = pytz.timezone("Europe/Moscow")  # used for display; DB stores UTC timestamps

# ---------------- Bot / Dispatcher ---------------
bot = Bot(token=BOT_TOKEN, timeout=30, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ---------------- DB (aiosqlite) --------------------
DB_FILE = "bot.db"
db: Optional[aiosqlite.Connection] = None

# db_lock for async coordination
db_lock: Optional[asyncio.Lock] = None

# ---------------- Pending actions (conversation shim, persistent) --------------
# In-memory cache plus DB-backed persistence.
pending_actions: Dict[int, Dict[str, Any]] = {}
pending_lock = asyncio.Lock()

async def set_pending(uid: int, action_type: str, data: Optional[Dict[str, Any]] = None):
    """
    Set pending action both in memory and persistently in DB.
    """
    async with pending_lock:
        pending_actions[uid] = {"type": action_type, "data": data or {}, "ts": int(time.time())}
    # persist
    try:
        await db_execute(
            "INSERT OR REPLACE INTO pending (user_id, type, data_json, created_at) VALUES (?, ?, ?, ?)",
            (uid, action_type, json.dumps(data or {}), int(time.time()))
        )
    except Exception:
        logger.exception("Failed to persist pending for %s", uid)
    logger.info("PENDING: set for %s -> %s", uid, action_type)

async def pop_pending(uid: int) -> Optional[Dict[str, Any]]:
    """
    Remove pending action from memory and DB, returning it.
    """
    async with pending_lock:
        p = pending_actions.pop(uid, None)
    try:
        await db_execute("DELETE FROM pending WHERE user_id = ?", (uid,))
    except Exception:
        logger.exception("Failed to delete pending for %s", uid)
    if p:
        logger.info("PENDING: popped for %s -> %s", uid, p.get("type"))
    return p

async def get_pending(uid: int) -> Optional[Dict[str, Any]]:
    async with pending_lock:
        return pending_actions.get(uid)

# ---------------- Keyword map and auto parsing ----------------
# Small tuned mapping of words -> category. Expandable.
KEYWORD_MAP = {
    "кафе": "Развлечения",
    "кофе": "Развлечения",
    "пицца": "Покупки",
    "супермаркет": "Продуктовая корзина",
    "магазин": "Покупки",
    "такси": "Транспорт",
    "метро": "Транспорт",
    "аренда": "Аренда жилья",
    "жкх": "Комм. услуги",
    "интернет": "Связь",
    "медицина": "Медицина",
    "аптека": "Медицина",
    "бензин": "Транспорт",
    "топливо": "Транспорт",
    "кино": "Развлечения",
    "покупка": "Покупки",
    "подарок": "Покупки",
    "инвести": "Инвестиции",
    "подушка": "Подушка безопасности",
}

AMOUNT_RE = re.compile(r"(-?\d+[.,]?\d*)")

# ---------------- Categories & states -------------
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

# ---------------- Helpers & DB access (aiosqlite) ------------
async def init_db():
    """
    Initialize aiosqlite connection, pragmas and tables.
    Called from init_app_for_runtime.
    Also performs lightweight migrations (add monthly_budget to users, create pending).
    """
    global db
    db = await aiosqlite.connect(DB_FILE)
    db.row_factory = aiosqlite.Row  # type: ignore[attr-defined]
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA foreign_keys=ON;")
    await db.commit()

    # create core tables if missing (users with monthly_budget column)
    await db.execute("""CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        income REAL DEFAULT 0,
        notifications BOOLEAN DEFAULT 1
    )""")
    # ensure monthly_budget column exists; if not -> ALTER TABLE ADD COLUMN
    rows = await db.execute("PRAGMA table_info(users)")
    cols = await rows.fetchall()
    col_names = [r[1] for r in cols]
    if "monthly_budget" not in col_names:
        try:
            await db.execute("ALTER TABLE users ADD COLUMN monthly_budget REAL DEFAULT NULL")
            await db.commit()
            logger.info("DB migration: added users.monthly_budget")
        except Exception:
            logger.exception("Failed to add monthly_budget column (may exist)")

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
    # pending table for persistent pending actions
    await db.execute("""CREATE TABLE IF NOT EXISTS pending (
        user_id INTEGER PRIMARY KEY,
        type TEXT,
        data_json TEXT,
        created_at INTEGER
    )""")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_expenses_user_timestamp ON expenses(user_id, timestamp)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_recurring_day ON recurring(day)")
    await db.commit()

    # load pending into memory
    try:
        cur = await db.execute("SELECT user_id, type, data_json FROM pending")
        rows = await cur.fetchall()
        await cur.close()
        if rows:
            async with pending_lock:
                for r in rows:
                    uid = int(r["user_id"])
                    t = r["type"]
                    d = {}
                    try:
                        d = json.loads(r["data_json"]) if r["data_json"] else {}
                    except Exception:
                        logger.debug("pending load: json parse failed for user %s", uid)
                    pending_actions[uid] = {"type": t, "data": d, "ts": int(time.time())}
            logger.info("Loaded %s pending actions from DB", len(rows))
    except Exception:
        logger.exception("Failed to load pending actions from DB (ignored)")

async def close_db():
    """Close DB connection cleanly."""
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
    """Execute modifying query and commit."""
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
async def ensure_user(uid: int):
    global db_lock
    if db_lock is None:
        db_lock = asyncio.Lock()
    await db_execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (uid,))

async def get_income(uid: int) -> float:
    r = await db_fetchone("SELECT income FROM users WHERE user_id = ?", (uid,))
    return float(r["income"]) if r and r["income"] is not None else 0.0

async def set_income(uid: int, v: float):
    await db_execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (uid,))
    await db_execute("UPDATE users SET income = ? WHERE user_id = ?", (v, uid))

async def get_monthly_budget(uid: int) -> Optional[float]:
    r = await db_fetchone("SELECT monthly_budget FROM users WHERE user_id = ?", (uid,))
    if not r:
        return None
    return float(r["monthly_budget"]) if r["monthly_budget"] is not None else None

async def set_monthly_budget(uid: int, amount: Optional[float]):
    await db_execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (uid,))
    await db_execute("UPDATE users SET monthly_budget = ? WHERE user_id = ?", (amount, uid))

def format_amount(x):
    try:
        return f"{x:,.0f}".replace(",", " ")
    except Exception:
        return str(x)

def get_limits_from_income(income: float):
    return {cat: income * pct for group in CATEGORIES.values() for cat, pct in group.items()}

async def get_month_spent(uid: int):
    now_utc = datetime.utcnow()
    month_start_dt = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_start = month_start_dt.isoformat()
    month_end_dt = (month_start_dt + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    month_end = month_end_dt.isoformat()
    r = await db_fetchone(
        "SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ?",
        (uid, month_start, month_end)
    )
    return r["total"] if r and r["total"] is not None else 0

async def add_expense(uid, amount, category, ts=None, rec_id=None):
    ts = ts or datetime.utcnow().isoformat()
    await db_execute(
        "INSERT INTO expenses (user_id, amount, category, timestamp, recurring_id) VALUES (?, ?, ?, ?, ?)",
        (uid, amount, category, ts, rec_id)
    )
    # check monthly budget and notify if exceeded
    try:
        bud = await get_monthly_budget(uid)
        if bud is not None:
            total = await get_month_spent(uid)
            if total > bud:
                try:
                    await bot.send_message(uid, f"⚠️ Ты превысил месячный бюджет ({format_amount(bud)} ₽). Текущие траты: {format_amount(total)} ₽")
                except Exception:
                    logger.debug("Failed to send budget exceeded msg to %s", uid)
            elif total > 0.9 * bud:
                try:
                    await bot.send_message(uid, f"⚠️ Ты израсходовал более 90% месячного бюджета ({format_amount(bud)} ₽). Текущие траты: {format_amount(total)} ₽")
                except Exception:
                    logger.debug("Failed to send budget warning to %s", uid)
    except Exception:
        logger.exception("Budget check after add_expense failed (ignored)")

async def get_expenses(uid, limit=10):
    rows = await db_fetchall(
        "SELECT id, amount, category, timestamp FROM expenses WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?",
        (uid, limit)
    )
    return rows

async def delete_expense(eid):
    await db_execute("DELETE FROM expenses WHERE id = ?", (eid,))

async def check_limits(uid, category, amount):
    limits = get_limits_from_income(await get_income(uid))
    if category not in limits:
        return []
    income = await get_income(uid)
    now_utc = datetime.utcnow()
    month_start_dt = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_start = month_start_dt.isoformat()
    month_end_dt = (month_start_dt + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    month_end = month_end_dt.isoformat()
    r_total = await db_fetchone(
        "SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ?",
        (uid, month_start, month_end)
    )
    total_spent = r_total["total"] if r_total and r_total["total"] is not None else 0
    r_cat = await db_fetchone(
        "SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND category = ? AND timestamp BETWEEN ? AND ?",
        (uid, category, month_start, month_end)
    )
    cat_spent = r_cat["total"] if r_cat and r_cat["total"] is not None else 0
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
    now_utc = datetime.utcnow()
    month_start_dt = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_start = month_start_dt.isoformat()
    month_end_dt = (month_start_dt + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    month_end = month_end_dt.isoformat()
    rows = await db_fetchall(
        "SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ? GROUP BY category",
        (uid, month_start, month_end)
    )
    spent = {r["category"]: r["total"] for r in rows}
    text = f"💰 Ваш доход: {format_amount(income)} ₽\n"
    bud = await get_monthly_budget(uid)
    if bud is not None:
        text += f"📌 Месячный бюджет: {format_amount(bud)} ₽\n"
    text += "\n"
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
    today = datetime.utcnow().day
    rows = await db_fetchall("SELECT id, user_id, amount, category FROM recurring WHERE day = ?", (today,))
    async def _handle_row(r):
        try:
            rec_id = r["id"]
            uid = r["user_id"]
            amt = r["amount"]
            cat = r["category"]
            await add_expense(uid, amt, cat, rec_id=rec_id)
            try:
                await bot.send_message(uid, f"🔁 Добавлен регулярный расход: {amt:,.0f} ₽ — {cat}")
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
def get_main_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("➕ Добавить трату", "📜 История")
    kb.add("📊 Моя статистика", "ℹ️ Помощь")
    return kb

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
    await ensure_user(uid)
    welcome = (
        "<b>Привет! Я — твой финансовый помощник.</b>\n\n"
        "Я помогу тебе отслеживать расходы, планировать бюджет, "
        "настраивать регулярные платежи и вовремя предупреждать о превышениях лимитов.\n\n"
        "Чтобы начать — введите ваш ежемесячный доход (например: <b>50 000</b>)\n\n"
        "После ввода дохода я рассчитую рекомендованные лимиты по категориям и покажу подсказки по кнопкам внизу."
    )
    kb = get_main_keyboard()

    # PENDING: set conversation shim to expect income input (persisted)
    await set_pending(uid, "income")
    try:
        await IncomeState.income.set()
    except Exception:
        logger.debug("start: IncomeState.income.set() failed (ignored)")

    await bot.send_message(msg.chat.id, welcome, reply_markup=kb)

@dp.message_handler(commands=['cancel'], state="*")
async def cmd_cancel(msg: types.Message, state: FSMContext):
    await pop_pending(msg.from_user.id)
    cur = await state.get_state()
    if cur is None:
        await bot.send_message(msg.chat.id, "Нечего отменять.")
        return
    await state.finish()
    await bot.send_message(msg.chat.id, "Действие отменено. Можешь использовать кнопки ниже.", reply_markup=get_main_keyboard())

# --- Generic text handler that first looks at pending_actions (PENDING) ---
@dp.message_handler(content_types=['text'])
async def generic_text_handler(msg: types.Message):
    uid = msg.from_user.id
    text = (msg.text or "").strip()
    if text.startswith("/"):
        return  # let command handlers process

    # --- Auto-parse: if user writes something containing a number, treat as quick add or pre-fill ---
    # This happens only when there is NO pending action (to avoid surprises).
    pending = await get_pending(uid)
    if not pending:
        m = AMOUNT_RE.search(text)
        if m:
            # parse amount
            try:
                amt_raw = m.group(1)
                amount = float(amt_raw.replace(",", "."))
                # find category by keyword in text
                lowered = text.lower()
                guessed = None
                for kw, cat in KEYWORD_MAP.items():
                    if kw in lowered:
                        guessed = cat
                        break
                if guessed:
                    # immediate add
                    await add_expense(uid, amount, guessed)
                    await bot.send_message(uid, f"✅ Добавлено: {format_amount(amount)} ₽ — {guessed}")
                    warnings = await check_limits(uid, guessed, amount)
                    if warnings:
                        await bot.send_message(uid, "\n".join(warnings))
                    return
                else:
                    # no category guessed -> set pending to choose category, prefill amount
                    await set_pending(uid, "expense_choose_category", {"amount": amount})
                    kb = InlineKeyboardMarkup(row_width=2)
                    for cat in ALL_CATEGORIES:
                        kb.insert(InlineKeyboardButton(cat, callback_data=f"cat_{cat}"))
                    await bot.send_message(uid, "Выбери категорию:", reply_markup=kb)
                    return
            except Exception:
                logger.debug("Auto-parse quick add failed for text: %s", text, exc_info=True)
                # fall through to normal flow

    # If the user has a pending action, handle it here (income/expense_amount/recurring flows)
    pending = await get_pending(uid)
    if pending:
        ptype = pending.get("type")
        pdata = pending.get("data", {})
        logger.info("PENDING: processing %s input from %s -> %s", ptype, uid, text[:50])
        if ptype == "income":
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
                await bot.send_message(uid, "❌ Неверный формат дохода. Введите число, например: 50 000 (или нажмите /cancel).")
            return
        elif ptype == "expense_amount":
            try:
                amount = float(text.replace(" ", "").replace(",", "."))
                pdata['amount'] = amount
                async with pending_lock:
                    pending_actions[uid]['data'] = pdata
                    pending_actions[uid]['type'] = "expense_choose_category"
                kb = InlineKeyboardMarkup(row_width=2)
                for cat in ALL_CATEGORIES:
                    kb.insert(InlineKeyboardButton(cat, callback_data=f"cat_{cat}"))
                await bot.send_message(uid, "Выбери категорию:", reply_markup=kb)
            except Exception:
                await bot.send_message(uid, "❌ Неверная сумма. Введите число, например: 450. Или нажмите /cancel, чтобы отменить.")
            return
        elif ptype == "expense_choose_category":
            # Shouldn't happen: user typed text while we expected category via inline; guide them
            await bot.send_message(uid, "Ожидается выбор категории через кнопку. Пожалуйста выберите категорию.")
            return
        elif ptype == "recurring_amount":
            try:
                amount = float(text.replace(" ", "").replace(",", "."))
                pdata['amount'] = amount
                async with pending_lock:
                    pending_actions[uid]['data'] = pdata
                    pending_actions[uid]['type'] = "recurring_choose_category"
                kb = InlineKeyboardMarkup(row_width=2)
                for cat in ALL_CATEGORIES:
                    kb.insert(InlineKeyboardButton(cat, callback_data=f"rec_{cat}"))
                await bot.send_message(uid, "Выбери категорию:", reply_markup=kb)
            except Exception:
                await bot.send_message(uid, "❌ Неверная сумма. Введите число или /cancel.")
            return
        elif ptype == "recurring_choose_category":
            await bot.send_message(uid, "Ожидается выбор категории через кнопку. Пожалуйста выберите категорию.")
            return
        elif ptype == "recurring_day":
            try:
                day = int(text)
                if not (1 <= day <= 28):
                    raise ValueError
                data = pdata
                await db_execute("INSERT INTO recurring (user_id, amount, category, day) VALUES (?, ?, ?, ?)",
                                 (uid, data["amount"], data["category"], day))
                await pop_pending(uid)
                await bot.send_message(uid, f"🔁 Регулярный расход сохранён: {format_amount(data['amount'])} ₽ — {data['category']} (каждое {day}-е число)")
            except Exception:
                await bot.send_message(uid, "❌ Укажи число от 1 до 28 или /cancel")
            return
        else:
            await pop_pending(uid)
            logger.warning("PENDING: unknown type %s for user %s - cleared", ptype, uid)
            await bot.send_message(uid, "Произошла ошибка, пожалуйста повторите действие.")
            return

    # If no pending action, handle main keyboard texts
    if text == "➕ Добавить трату":
        await set_pending(uid, "expense_amount")
        try:
            await ExpenseState.amount.set()
        except Exception:
            logger.debug("ExpenseState.amount.set() failed (ignored)")
        await bot.send_message(uid, "💸 Введи сумму траты (например: 450): (или /cancel чтобы отменить)")
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

    await bot.send_message(uid, "Не понял. Используйте кнопки или /start, /help.")

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
                await cb.message.edit_text("💸 Введи сумму траты (например: 450): (или /cancel чтобы отменить)")
            except Exception:
                pass
            async with pending_lock:
                pending_actions[uid] = {"type": "expense_amount", "data": {}}
                # persist as well
                try:
                    await db_execute("INSERT OR REPLACE INTO pending (user_id, type, data_json, created_at) VALUES (?, ?, ?, ?)",
                                     (uid, "expense_amount", json.dumps({}), int(time.time())))
                except Exception:
                    logger.exception("Failed to persist pending change to expense_amount")
            return
        await add_expense(uid, amount, cat)
        await pop_pending(uid)
        try:
            await cb.message.edit_text(f"✅ Добавлено: {format_amount(amount)} ₽ — {cat}")
        except Exception:
            await bot.send_message(uid, f"✅ Добавлено: {format_amount(amount)} ₽ — {cat}")
        warnings = await check_limits(uid, cat, amount)
        if warnings:
            await bot.send_message(uid, "\n".join(warnings))
        return
    else:
        await cb.answer("Сначала укажите сумму траты.")
        try:
            await cb.message.edit_text("💸 Введи сумму траты (например: 450): (или /cancel чтобы отменить)")
        except Exception:
            pass
        await set_pending(uid, "expense_amount")
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
                await cb.message.edit_text("Введи сумму регулярного расхода (или /cancel):")
            except Exception:
                pass
            async with pending_lock:
                pending_actions[uid] = {"type": "recurring_amount", "data": {}}
                try:
                    await db_execute("INSERT OR REPLACE INTO pending (user_id, type, data_json, created_at) VALUES (?, ?, ?, ?)",
                                     (uid, "recurring_amount", json.dumps({}), int(time.time())))
                except Exception:
                    logger.exception("Failed to persist pending change to recurring_amount")
            return
        async with pending_lock:
            pending_actions[uid] = {"type": "recurring_day", "data": {"amount": amount, "category": cat}, "ts": int(time.time())}
            try:
                await db_execute("INSERT OR REPLACE INTO pending (user_id, type, data_json, created_at) VALUES (?, ?, ?, ?)",
                                 (uid, "recurring_day", json.dumps({"amount": amount, "category": cat}), int(time.time())))
            except Exception:
                logger.exception("Failed to persist pending change to recurring_day")
        try:
            await cb.message.edit_text("Укажи день месяца (1–28):")
        except Exception:
            await bot.send_message(uid, "Укажи день месяца (1–28):")
        return
    else:
        await cb.answer("Сначала укажите сумму регулярного расхода.")
        try:
            await cb.message.edit_text("Введи сумму регулярного расхода (или /cancel):")
        except Exception:
            pass
        await set_pending(uid, "recurring_amount")
        return

# ---------------- Other handlers (history, delete, stats, help, notify, add_recurring, report, budget) ----------------
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
    await bot.send_message(msg.chat.id,
        "/report week — отчёт за неделю\n"
        "/report month — отчёт за месяц\n"
        "/add_recurring — добавить регулярный расход\n"
        "/notify — включить/выключить уведомления\n"
        "/set_budget <сумма> — установить месячный бюджет\n"
        "/budget — показать текущий бюджет\n"
        "/cancel — отменить текущее действие"
    )

@dp.message_handler(commands=['notify'])
async def toggle_notify(msg: types.Message):
    uid = msg.from_user.id
    r = await db_fetchone("SELECT notifications FROM users WHERE user_id = ?", (uid,))
    current = bool(r['notifications']) if r else True
    new_val = 0 if current else 1
    await db_execute("UPDATE users SET notifications = ? WHERE user_id = ?", (new_val, uid))
    await bot.send_message(msg.chat.id, "🔔 Уведомления включены" if new_val else "🔕 Уведомления отключены")

@dp.message_handler(commands=['add_recurring'])
async def add_recurring(msg: types.Message):
    uid = msg.from_user.id
    await set_pending(uid, "recurring_amount")
    try:
        await RecurringState.amount.set()
    except Exception:
        logger.debug("RecurringState.amount.set() failed (ignored)")
    await bot.send_message(msg.chat.id, "Введи сумму регулярного расхода (или /cancel):")

@dp.message_handler(state=RecurringState.amount)
async def recurring_amount(msg: types.Message, state: FSMContext):
    text = msg.text or ""
    try:
        amt = float(text.replace(" ", "").replace(",", "."))
        await state.update_data(amount=amt)
        kb = InlineKeyboardMarkup(row_width=2)
        for cat in ALL_CATEGORIES:
            kb.insert(InlineKeyboardButton(cat, callback_data=f"rec_{cat}"))
        await bot.send_message(msg.chat.id, "Выбери категорию:", reply_markup=kb)
        try:
            await RecurringState.category.set()
        except Exception:
            logger.debug("RecurringState.category.set() failed (ignored)")
    except Exception:
        await bot.send_message(msg.chat.id, "❌ Неверная сумма. Введите число или /cancel.")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('rec_'), state='*')
async def recurring_category_fallback(cb: types.CallbackQuery):
    await recurring_category(cb)

@dp.message_handler(commands=['report'])
async def report_cmd(msg: types.Message):
    args = msg.get_args().strip().lower()
    if args not in ('week', 'month'):
        await bot.send_message(msg.chat.id, "Используй: /report week или /report month")
        return
    now = datetime.utcnow()
    start = now - timedelta(days=7) if args == 'week' else now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    rows = await db_fetchall("SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp >= ? GROUP BY category",
                   (msg.from_user.id, start.isoformat()))
    if not rows:
        await bot.send_message(msg.chat.id, "Нет данных за выбранный период.")
        return
    text = f"📊 Отчёт за {'неделю' if args == 'week' else 'месяц'}:\n\n"
    for r in rows:
        total = r["total"] if r and r["total"] is not None else 0
        text += f"{r['category']}: {total:,.0f} ₽\n"
    await bot.send_message(msg.chat.id, text)

# ---------------- Budget commands ----------------
@dp.message_handler(commands=['set_budget'])
async def cmd_set_budget(msg: types.Message):
    uid = msg.from_user.id
    arg = msg.get_args().strip().replace(" ", "").replace(",", ".")
    if not arg:
        await bot.send_message(msg.chat.id, "Используй: /set_budget <сумма> (например: /set_budget 50000) или /set_budget off чтобы убрать бюджет.")
        return
    if arg.lower() in ("off", "none", "0"):
        await set_monthly_budget(uid, None)
        await bot.send_message(msg.chat.id, "Месячный бюджет удалён.")
        return
    try:
        amt = float(arg)
        await set_monthly_budget(uid, amt)
        await bot.send_message(msg.chat.id, f"Месячный бюджет установлен: {format_amount(amt)} ₽")
    except Exception:
        await bot.send_message(msg.chat.id, "Неверный формат суммы. Используй: /set_budget 50000")

@dp.message_handler(commands=['budget'])
async def cmd_budget(msg: types.Message):
    uid = msg.from_user.id
    bud = await get_monthly_budget(uid)
    if bud is None:
        await bot.send_message(msg.chat.id, "Месячный бюджет не установлен.")
        return
    total = await get_month_spent(uid)
    await bot.send_message(msg.chat.id, f"Месячный бюджет: {format_amount(bud)} ₽\nРасходы за месяц: {format_amount(total)} ₽")

# ---------------- Init helper to be called from main.py on startup ------------
async def init_app_for_runtime(app):
    """
    Called from main.py on startup to initialize DB connection, scheduler jobs, etc.
    """
    global db_lock
    if db_lock is None:
        db_lock = asyncio.Lock()

    # init DB connection and tables + migrate
    await init_db()

    # scheduler jobs
    _add_scheduler_jobs_once()
    try:
        scheduler.start()
        logger.info("Scheduler started (bot_app)")
    except Exception:
        logger.exception("Failed to start scheduler (bot_app)")

    # pre-create bot session (best-effort)
    try:
        sess = await bot.get_session()
        app['bot_session'] = sess
    except Exception:
        logger.debug("bot.get_session() failed during bot_app init (may be fine)")

# Exported names for main.py convenience
__all__ = ("bot", "dp", "scheduler", "init_app_for_runtime", "get_main_keyboard", "format_stats", "close_db",
           "db_fetchone", "db_fetchall", "db_execute", "get_month_spent")
