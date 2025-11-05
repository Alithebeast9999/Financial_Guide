#!/usr/bin/env python3
"""
Financial Assistant Telegram Bot (Webhook + Render + SQLite + APScheduler)
"""

import os
import logging
import sqlite3
from datetime import datetime, timedelta
import pytz
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor

# === ЛОГИРОВАНИЕ ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === КОНФИГ ===
BOT_TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
TZ = pytz.timezone("Europe/Moscow")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен!")

# === БАЗА ДАННЫХ ===
DB_FILE = "bot.db"
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    income REAL DEFAULT 0,
    notifications BOOLEAN DEFAULT TRUE
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

# === КАТЕГОРИИ ===
CATEGORIES = {
    "НАДО": {
        "Аренда жилья": 0.35,
        "Продуктовая корзина": 0.15,
        "Комм. услуги": 0.05,
        "Связь": 0.03,
        "Транспорт": 0.05,
        "Личный уход": 0.02,
        "Медицина": 0.08
    },
    "МОГУ": {
        "Инвестиции": 0.05,
        "Подушка безопасности": 0.05
    },
    "ХОЧУ": {
        "Развлечения": 0.07,
        "Отдых - путешествия": 0.05,
        "Покупки": 0.05
    }
}
ALL_CATEGORIES = [c for g in CATEGORIES.values() for c in g]

# === БОТ ===
bot = Bot(token=BOT_TOKEN, timeout=30)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
db_lock = asyncio.Lock()

# === СОСТОЯНИЯ ===
class IncomeState(StatesGroup):
    income = State()

class ExpenseState(StatesGroup):
    amount = State()
    category = State()

class RecurringState(StatesGroup):
    amount = State()
    category = State()
    day = State()

# === УТИЛИТЫ ===
async def ensure_user(user_id):
    async with db_lock:
        cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()

def get_income(user_id):
    cursor.execute("SELECT income FROM users WHERE user_id = ?", (user_id,))
    res = cursor.fetchone()
    return res["income"] if res else 0

def set_income(user_id, income):
    cursor.execute("INSERT OR REPLACE INTO users (user_id, income) VALUES (?, ?)", (user_id, income))
    conn.commit()

def get_limits(user_id):
    income = get_income(user_id)
    return {cat: income * pct for group in CATEGORIES.values() for cat, pct in group.items()}

def add_expense(user_id, amount, category, ts=None, rec_id=None):
    ts = ts or datetime.now(TZ)
    cursor.execute("INSERT INTO expenses (user_id, amount, category, timestamp, recurring_id) VALUES (?, ?, ?, ?, ?)",
                   (user_id, amount, category, ts, rec_id))
    conn.commit()

def get_expenses(user_id, limit=10):
    cursor.execute("SELECT id, amount, category, timestamp FROM expenses WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?", (user_id, limit))
    return cursor.fetchall()

def delete_expense(exp_id):
    cursor.execute("DELETE FROM expenses WHERE id = ?", (exp_id,))
    conn.commit()

def check_limits(user_id, category, amount):
    limits = get_limits(user_id)
    if category not in limits:
        return []

    income = get_income(user_id)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    cursor.execute("SELECT SUM(amount) FROM expenses WHERE user_id=? AND timestamp BETWEEN ? AND ?", (user_id, month_start, month_end))
    total_spent = cursor.fetchone()[0] or 0
    cursor.execute("SELECT SUM(amount) FROM expenses WHERE user_id=? AND category=? AND timestamp BETWEEN ? AND ?", (user_id, category, month_start, month_end))
    cat_spent = cursor.fetchone()[0] or 0

    msgs = []
    if total_spent + amount > income:
        msgs.append("⚠️ Общий месячный лимит превышен!")
    if cat_spent + amount > limits[category]:
        msgs.append(f"⚠️ Лимит по '{category}' превышен!")
    elif cat_spent + amount > 0.9 * limits[category]:
        msgs.append(f"⚠️ Ты израсходовал более 90% лимита по '{category}'!")
    return msgs

def format_stats(user_id):
    income = get_income(user_id)
    limits = get_limits(user_id)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    cursor.execute("SELECT category, SUM(amount) as total FROM expenses WHERE user_id=? AND timestamp BETWEEN ? AND ? GROUP BY category",
                   (user_id, month_start, month_end))
    spent = {r["category"]: r["total"] for r in cursor.fetchall()}

    text = f"💰 Ваш доход: {income:,.0f} ₽\n\n"
    for group, cats in CATEGORIES.items():
        text += f"📂 {group}\n"
        for cat, pct in cats.items():
            lim = limits[cat]
            s = spent.get(cat, 0)
            perc = (s / lim * 100) if lim else 0
            text += f"• {cat}: {s:,.0f} ₽ / {lim:,.0f} ₽ ({perc:.0f}%)\n"
        text += "\n"
    return text

# === ПЛАНИРОВЩИК ===
scheduler = AsyncIOScheduler(timezone=TZ)

async def daily_reminders():
    cursor.execute("SELECT user_id FROM users WHERE notifications=TRUE")
    for (uid,) in cursor.fetchall():
        try:
            await bot.send_message(uid, "💡 Не забудь добавить траты за сегодня!")
        except Exception as e:
            logger.error(e)

async def weekly_report():
    cursor.execute("SELECT user_id FROM users")
    for (uid,) in cursor.fetchall():
        try:
            await bot.send_message(uid, "📊 Еженедельный отчёт:\n\n" + format_stats(uid))
        except Exception as e:
            logger.error(e)

async def process_recurring():
    today = datetime.now(TZ).day
    cursor.execute("SELECT id, user_id, amount, category FROM recurring WHERE day=?", (today,))
    for rec_id, uid, amt, cat in cursor.fetchall():
        add_expense(uid, amt, cat, rec_id=rec_id)
        try:
            await bot.send_message(uid, f"🔁 Добавлен регулярный расход: {amt:,.0f} ₽ — {cat}")
        except Exception as e:
            logger.error(e)

scheduler.add_job(daily_reminders, CronTrigger(hour=9, minute=0))
scheduler.add_job(weekly_report, CronTrigger(day_of_week="mon", hour=9, minute=0))
scheduler.add_job(process_recurring, CronTrigger(hour=6, minute=0))

# === ХЕНДЛЕРЫ ===
@dp.message_handler(commands=["start"])
async def start(msg: types.Message):
    uid = msg.from_user.id
    await ensure_user(uid)
    if get_income(uid) == 0:
        await msg.reply("👋 Привет! Я твой финансовый помощник.\n\nВведи ежемесячный доход (например: 100000):")
        await IncomeState.income.set()
    else:
        await show_menu(msg)

@dp.message_handler(state=IncomeState.income)
async def set_income_handler(msg: types.Message, state: FSMContext):
    try:
        income = float(msg.text.replace(" ", "").replace(",", "."))
        set_income(msg.from_user.id, income)
        await state.finish()
        await msg.reply(f"Доход сохранён: {income:,.0f} ₽ ✅\nТеперь лимиты рассчитаны автоматически!")
        await show_menu(msg)
    except:
        await msg.reply("❌ Неверный формат. Пример: 100000")

async def show_menu(msg: types.Message):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("➕ Добавить трату", "📜 История")
    kb.add("📊 Моя статистика", "ℹ️ Помощь")
    await msg.reply("Главное меню:", reply_markup=kb)

# === Добавление трат ===
@dp.message_handler(Text(equals="➕ Добавить трату"))
async def add_expense_cmd(msg: types.Message):
    await msg.reply("💸 Введи сумму траты:")
    await ExpenseState.amount.set()

@dp.message_handler(state=ExpenseState.amount)
async def expense_amount(msg: types.Message, state: FSMContext):
    try:
        amount = float(msg.text.replace(" ", "").replace(",", "."))
        await state.update_data(amount=amount)
        kb = InlineKeyboardMarkup(row_width=2)
        for cat in ALL_CATEGORIES:
            kb.insert(InlineKeyboardButton(cat, callback_data=f"cat_{cat}"))
        await msg.reply("Выбери категорию:", reply_markup=kb)
        await ExpenseState.category.set()
    except:
        await msg.reply("❌ Неверная сумма")

@dp.callback_query_handler(lambda c: c.data.startswith("cat_"), state=ExpenseState.category)
async def expense_category(cb: types.CallbackQuery, state: FSMContext):
    cat = cb.data[4:]
    data = await state.get_data()
    amount = data["amount"]
    uid = cb.from_user.id
    warnings = check_limits(uid, cat, amount)
    add_expense(uid, amount, cat)
    await cb.message.edit_text(f"✅ Добавлено: {amount:,.0f} ₽ — {cat}")
    if warnings:
        await bot.send_message(uid, "\n".join(warnings))
    await state.finish()

# === История ===
@dp.message_handler(Text(equals="📜 История"))
async def history(msg: types.Message):
    exps = get_expenses(msg.from_user.id)
    if not exps:
        await msg.reply("Пока нет трат 💰")
        return
    for e in exps:
        dt = datetime.fromisoformat(e["timestamp"]).strftime("%d.%m %H:%M")
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Удалить", callback_data=f"del_{e['id']}"))
        await msg.reply(f"{dt} | {e['amount']:,.0f} ₽ | {e['category']}", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("del_"))
async def delete_expense_cb(cb: types.CallbackQuery):
    eid = int(cb.data[4:])
    delete_expense(eid)
    await cb.answer("Удалено")
    await cb.message.delete()

# === Статистика ===
@dp.message_handler(Text(equals="📊 Моя статистика"))
async def stats(msg: types.Message):
    await msg.reply(format_stats(msg.from_user.id))

# === Помощь ===
@dp.message_handler(Text(equals="ℹ️ Помощь"))
async def help_cmd(msg: types.Message):
    await msg.reply(
        "/report week — отчёт за неделю\n"
        "/report month — отчёт за месяц\n"
        "/add_recurring — добавить регулярный расход\n"
        "/notify — включить/выключить уведомления\n"
    )

# === Уведомления ===
@dp.message_handler(commands=["notify"])
async def toggle_notify(msg: types.Message):
    uid = msg.from_user.id
    cursor.execute("SELECT notifications FROM users WHERE user_id=?", (uid,))
    cur = cursor.fetchone()
    new_val = not cur["notifications"]
    cursor.execute("UPDATE users SET notifications=? WHERE user_id=?", (new_val, uid))
    conn.commit()
    await msg.reply("🔔 Уведомления включены" if new_val else "🔕 Уведомления отключены")

# === Recurring ===
@dp.message_handler(commands=["add_recurring"])
async def add_recurring(msg: types.Message):
    await msg.reply("Введи сумму регулярного расхода:")
    await RecurringState.amount.set()

@dp.message_handler(state=RecurringState.amount)
async def recurring_amount(msg: types.Message, state: FSMContext):
    try:
        amt = float(msg.text.replace(" ", "").replace(",", "."))
        await state.update_data(amount=amt)
        kb = InlineKeyboardMarkup(row_width=2)
        for cat in ALL_CATEGORIES:
            kb.insert(InlineKeyboardButton(cat, callback_data=f"rec_{cat}"))
        await msg.reply("Выбери категорию:", reply_markup=kb)
        await RecurringState.category.set()
    except:
        await msg.reply("❌ Неверная сумма")

@dp.callback_query_handler(lambda c: c.data.startswith("rec_"), state=RecurringState.category)
async def recurring_category(cb: types.CallbackQuery, state: FSMContext):
    cat = cb.data[4:]
    await state.update_data(category=cat)
    await cb.message.edit_text("Укажи день месяца (1–28):")
    await RecurringState.day.set()

@dp.message_handler(state=RecurringState.day)
async def recurring_day(msg: types.Message, state: FSMContext):
    try:
        day = int(msg.text)
        if not (1 <= day <= 28):
            raise ValueError
        data = await state.get_data()
        cursor.execute("INSERT INTO recurring (user_id, amount, category, day) VALUES (?, ?, ?, ?)",
                       (msg.from_user.id, data["amount"], data["category"], day))
        conn.commit()
        await msg.reply(f"🔁 Регулярный расход сохранён: {data['amount']:,.0f} ₽ — {data['category']} (каждое {day}-е число)")
        await state.finish()
    except:
        await msg.reply("❌ Укажи число от 1 до 28")

# === Отчёты ===
@dp.message_handler(commands=["report"])
async def report_cmd(msg: types.Message):
    args = msg.get_args().strip().lower()
    if args not in ("week", "month"):
        await msg.reply("Используй: /report week или /report month")
        return
    now = datetime.now(TZ)
    start = now - timedelta(days=7) if args == "week" else now.replace(day=1)
    cursor.execute("SELECT category, SUM(amount) as total FROM expenses WHERE user_id=? AND timestamp >= ? GROUP BY category",
                   (msg.from_user.id, start))
    data = cursor.fetchall()
    if not data:
        await msg.reply("Нет данных за выбранный период.")
        return
    text = f"📊 Отчёт за {'неделю' if args == 'week' else 'месяц'}:\n\n"
    for row in data:
        text += f"{row['category']}: {row['total']:,.0f} ₽\n"
    await msg.reply(text)

# === ЗАПУСК ===
async def on_startup(dp):
    if WEBHOOK_URL:
        webhook_url = WEBHOOK_URL.rstrip("/")
        await bot.set_webhook(f"{webhook_url}/webhook")
        logger.info(f"Webhook установлен: {webhook_url}/webhook")
    scheduler.start()

async def on_shutdown(dp):
    await bot.delete_webhook()
    await dp.storage.close()
    await dp.storage.wait_closed()
    scheduler.shutdown()
    logger.info("Bot stopped")

if __name__ == "__main__":
    if WEBHOOK_URL:
        executor.start_webhook(
            dispatcher=dp,
            webhook_path="/webhook",
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            skip_updates=True,
            host="0.0.0.0",
            port=int(os.environ.get("PORT", 10000)),
        )
    else:
        executor.start_polling(dp, skip_updates=True)
