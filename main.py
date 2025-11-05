import os
import logging
import sqlite3
from datetime import datetime, timedelta
import pytz
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
BOT_TOKEN = os.environ.get('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен!")

WEBHOOK_URL = os.environ.get('WEBHOOK_URL')
if not WEBHOOK_URL:
    logger.warning("WEBHOOK_URL не установлен — будет использован polling (для теста)")

TZ = pytz.timezone('Europe/Moscow')

# === БАЗА ДАННЫХ ===
DB_FILE = 'bot.db'
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    income REAL DEFAULT 0,
    notifications BOOLEAN DEFAULT TRUE
)
''')
cursor.execute('''
CREATE TABLE IF NOT EXISTS expenses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    category TEXT,
    timestamp DATETIME,
    recurring_id INTEGER DEFAULT NULL
)
''')
cursor.execute('''
CREATE TABLE IF NOT EXISTS recurring (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    category TEXT,
    day INTEGER
)
''')
conn.commit()

# === КАТЕГОРИИ ===
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
ALL_CATEGORIES = [cat for group in CATEGORIES.values() for cat in group]

# === БОТ ===
bot = Bot(token=BOT_TOKEN, timeout=30)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# === СОСТОЯНИЯ ===
class IncomeState(StatesGroup): income = State()
class ExpenseState(StatesGroup): amount = State(); category = State()
class RecurringState(StatesGroup): amount = State(); category = State(); day = State()

# === УТИЛИТЫ ===
def ensure_user(user_id):
    cursor.execute('INSERT OR IGNORE INTO users (user_id) VALUES (?)', (user_id,))
    conn.commit()

def get_income(user_id):
    ensure_user(user_id)
    cursor.execute('SELECT income FROM users WHERE user_id = ?', (user_id,))
    return cursor.fetchone()[0]

def set_income(user_id, income):
    ensure_user(user_id)
    cursor.execute('INSERT OR REPLACE INTO users (user_id, income) VALUES (?, ?)', (user_id, income))
    conn.commit()

def get_limits(user_id):
    income = get_income(user_id)
    return {cat: income * pct for group in CATEGORIES.values() for cat, pct in group.items()}

def add_expense(user_id, amount, category, ts=None, rec_id=None):
    ts = ts or datetime.now(TZ)
    cursor.execute('INSERT INTO expenses (user_id, amount, category, timestamp, recurring_id) VALUES (?, ?, ?, ?, ?)',
                   (user_id, amount, category, ts, rec_id))
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
    cursor.execute('SELECT SUM(amount) FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ?', (user_id, month_start, month_end))
    total_spent = cursor.fetchone()[0] or 0
    cursor.execute('SELECT SUM(amount) FROM expenses WHERE user_id = ? AND category = ? AND timestamp BETWEEN ? AND ?', (user_id, category, month_start, month_end))
    cat_spent = cursor.fetchone()[0] or 0
    msgs = []
    if total_spent + amount > income:
        msgs.append("Общий месячный лимит превышен!")
    if cat_spent + amount > limits[category]:
        msgs.append(f"Лимит по '{category}' превышен!")
    return msgs

# === СТАТИСТИКА ===
def format_stats(user_id):
    income = get_income(user_id)
    limits = get_limits(user_id)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    cursor.execute('SELECT category, SUM(amount) FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ? GROUP BY category', (user_id, month_start, month_end))
    spent = dict(cursor.fetchall())
    text = f"ВАШ ДОХОД: {income:,.0f} ₽\n\n"
    for group, cats in CATEGORIES.items():
        text += f"{group}\n"
        for cat, pct in cats.items():
            lim = limits[cat]
            s = spent.get(cat, 0)
            text += f"{cat}: {pct*100:>3}% {lim:>8,.0f} ₽ (потрачено: {s:,.0f} ₽)\n"
        text += "\n"
    return text

# === ПЛАНИРОВЩИК ===
scheduler = AsyncIOScheduler(timezone=TZ)

async def daily_reminders():
    cursor.execute('SELECT user_id FROM users WHERE notifications = TRUE')
    for (uid,) in cursor.fetchall():
        try:
            await bot.send_message(uid, "Не забудь добавить траты за сегодня!")
        except: pass

async def weekly_report():
    cursor.execute('SELECT user_id FROM users')
    for (uid,) in cursor.fetchall():
        try:
            await bot.send_message(uid, "Еженедельный отчёт скоро...")
        except: pass

scheduler.add_job(daily_reminders, CronTrigger(hour=9, minute=0))
scheduler.add_job(weekly_report, CronTrigger(day_of_week='mon', hour=9, minute=0))

# === ХЕНДЛЕРЫ ===
@dp.message_handler(commands=['start'])
async def start(msg: types.Message):
    uid = msg.from_user.id
    ensure_user(uid)
    if get_income(uid) == 0:
        await msg.reply(
            "Привет! Я твой финансовый помощник.\n\n"
            "Введи ежемесячный доход (например: 100 000):"
        )
        await IncomeState.income.set()
    else:
        await show_menu(msg)

@dp.message_handler(state=IncomeState.income)
async def set_income_handler(msg: types.Message, state: FSMContext):
    try:
        income = float(msg.text.replace(' ', '').replace(',', '.'))
        set_income(msg.from_user.id, income)
        await state.finish()
        await msg.reply(f"Доход: {income:,.0f} ₽\nЛимиты рассчитаны!")
        await show_menu(msg)
    except:
        await msg.reply("Неверный формат. Пример: 100000")

async def show_menu(msg: types.Message):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("Добавить трату", "История")
    kb.add("Моя статистика", "Помощь")
    await msg.reply("Главное меню:", reply_markup=kb)

@dp.message_handler(Text(equals="Добавить трату"))
async def add_expense(msg: types.Message):
    await msg.reply("Сумма:")
    await ExpenseState.amount.set()

@dp.message_handler(state=ExpenseState.amount)
async def expense_amount(msg: types.Message, state: FSMContext):
    try:
        amount = float(msg.text.replace(' ', '').replace(',', '.'))
        await state.update_data(amount=amount)
        kb = InlineKeyboardMarkup(row_width=2)
        for cat in ALL_CATEGORIES:
            kb.add(InlineKeyboardButton(cat, callback_data=f"cat_{cat}"))
        await msg.reply("Категория:", reply_markup=kb)
        await ExpenseState.category.set()
    except:
        await msg.reply("Неверная сумма")

@dp.callback_query_handler(lambda c: c.data.startswith('cat_'), state=ExpenseState.category)
async def expense_category(cb: types.CallbackQuery, state: FSMContext):
    cat = cb.data[4:]
    data = await state.get_data()
    amount = data['amount']
    uid = cb.from_user.id
    warnings = check_limits(uid, cat, amount)
    add_expense(uid, amount, cat)
    await cb.message.edit_text(f"Добавлено: {amount:,.0f} ₽ — {cat}")
    if warnings:
        await bot.send_message(uid, "\n".join(warnings))
    await state.finish()

@dp.message_handler(Text(equals="Моя статистика"))
async def stats(msg: types.Message):
    await msg.reply(format_stats(msg.from_user.id))

@dp.message_handler(Text(equals="История"))
async def history(msg: types.Message):
    expenses = get_expenses(msg.from_user.id)
    if not expenses:
        await msg.reply("Нет трат")
        return
    for eid, amt, cat, ts in expenses:
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("Удалить", callback_data=f"del_{eid}"))
        await msg.reply(f"{ts[:16]} | {amt:,.0f} ₽ | {cat}", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith('del_'))
async def delete(cb: types.CallbackQuery):
    eid = int(cb.data[4:])
    delete_expense(eid)
    await cb.answer("Удалено")
    await cb.message.delete()

@dp.message_handler(Text(equals="Помощь"))
async def help_cmd(msg: types.Message):
    await msg.reply("/report week — отчёт\n/add_recurring — регулярные траты")

# === ЗАПУСК ===
async def on_startup(dp):
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info(f"Webhook: {WEBHOOK_URL}")
    scheduler.start()

if __name__ == '__main__':
    if WEBHOOK_URL:
        executor.start_webhook(
            dispatcher=dp,
            webhook_path='/',
            on_startup=on_startup,
            skip_updates=True,
            host='0.0.0.0',
            port=int(os.environ.get('PORT', 10000))
        )
    else:
        # Для локального теста
        executor.start_polling(dp, skip_updates=True)
