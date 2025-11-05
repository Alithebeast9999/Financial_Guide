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
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.utils import executor

# Logging setup
logging.basicConfig(level=logging.INFO)

# Environment variables
BOT_TOKEN = os.environ.get('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set in environment")

# Timezone
TZ = pytz.timezone('Europe/Moscow')  # UTC+3

# Database setup
DB_FILE = 'bot.db'
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()

# Create tables
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
    recurring_id INTEGER DEFAULT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS recurring (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    category TEXT,
    day INTEGER,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
)
''')

conn.commit()

# Categories and percentages from the screenshot
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

# Flatten categories for validation
ALL_CATEGORIES = [cat for group in CATEGORIES.values() for cat in group]

# Bot and dispatcher
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# States
class IncomeState(StatesGroup):
    income = State()

class ExpenseState(StatesGroup):
    amount = State()
    category = State()

class RecurringState(StatesGroup):
    amount = State()
    category = State()
    day = State()

# Helper functions
def get_user_income(user_id):
    cursor.execute('SELECT income FROM users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    return result[0] if result else 0

def set_user_income(user_id, income):
    cursor.execute('INSERT OR REPLACE INTO users (user_id, income) VALUES (?, ?)', (user_id, income))
    conn.commit()

def get_limits(user_id):
    income = get_user_income(user_id)
    limits = {}
    for group, cats in CATEGORIES.items():
        for cat, percent in cats.items():
            limits[cat] = income * percent
    return limits

def get_current_expenses(user_id, start_date, end_date):
    cursor.execute('''
    SELECT category, SUM(amount) FROM expenses
    WHERE user_id = ? AND timestamp BETWEEN ? AND ?
    GROUP BY category
    ''', (user_id, start_date, end_date))
    return dict(cursor.fetchall())

def add_expense(user_id, amount, category, timestamp=None, recurring_id=None):
    if timestamp is None:
        timestamp = datetime.now(TZ)
    cursor.execute('''
    INSERT INTO expenses (user_id, amount, category, timestamp, recurring_id)
    VALUES (?, ?, ?, ?, ?)
    ''', (user_id, amount, category, timestamp, recurring_id))
    conn.commit()
    return cursor.lastrowid

def get_expenses(user_id):
    cursor.execute('''
    SELECT id, amount, category, timestamp FROM expenses
    WHERE user_id = ? ORDER BY timestamp DESC
    ''', (user_id,))
    return cursor.fetchall()

def delete_expense(expense_id):
    cursor.execute('DELETE FROM expenses WHERE id = ?', (expense_id,))
    conn.commit()

def add_recurring(user_id, amount, category, day):
    cursor.execute('''
    INSERT INTO recurring (user_id, amount, category, day)
    VALUES (?, ?, ?, ?)
    ''', (user_id, amount, category, day))
    conn.commit()

def get_recurring(user_id):
    cursor.execute('SELECT id, amount, category, day FROM recurring WHERE user_id = ?', (user_id,))
    return cursor.fetchall()

def get_notifications_enabled(user_id):
    cursor.execute('SELECT notifications FROM users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    return result[0] if result else True

def toggle_notifications(user_id):
    current = get_notifications_enabled(user_id)
    new = not current
    cursor.execute('UPDATE users SET notifications = ? WHERE user_id = ?', (new, user_id))
    conn.commit()
    return new

def check_and_notify_limits(user_id, category, amount):
    limits = get_limits(user_id)
    if category not in limits:
        return
    income = get_user_income(user_id)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=32)).replace(day=1) - timedelta(seconds=1)
    current = get_current_expenses(user_id, month_start, month_end)
    total_spent = sum(current.values()) + amount
    cat_spent = current.get(category, 0) + amount
    messages = []
    if total_spent > income:
        messages.append("Внимание: Общий месячный лимит превышен!")
    if cat_spent > limits[category]:
        messages.append(f"Внимание: Лимит по категории '{category}' превышен!")
    return messages

def format_statistics(user_id):
    income = get_user_income(user_id)
    limits = get_limits(user_id)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=32)).replace(day=1) - timedelta(seconds=1)
    current = get_current_expenses(user_id, month_start, month_end)
    text = f"ВАШ ДОХОД: {income:.0f} ₽\n\n"
    for group, cats in CATEGORIES.items():
        text += f"{group}\n"
        for cat, percent in cats.items():
            lim = limits[cat]
            spent = current.get(cat, 0)
            text += f"{cat}: {percent*100}% {lim:.0f} ₽ (потрачено: {spent:.0f} ₽)\n"
        text += "\n"
    return text

def format_report(user_id, period):
    now = datetime.now(TZ)
    if period == 'week':
        start = now - timedelta(days=now.weekday() + 7)
        end = start + timedelta(days=7)
    elif period == 'month':
        start = now.replace(day=1)
        end = (start + timedelta(days=32)).replace(day=1) - timedelta(seconds=1)
    else:
        return "Неверный период"
    current = get_current_expenses(user_id, start, end)
    text = f"Отчет за {period}:\n"
    total = 0
    for cat, amt in current.items():
        text += f"{cat}: {amt:.0f} ₽\n"
        total += amt
    text += f"Итого: {total:.0f} ₽"
    return text

# Scheduler
scheduler = AsyncIOScheduler(timezone=TZ)

async def daily_tasks():
    now = datetime.now(TZ)
    day = now.day
    
    # Add recurring expenses
    cursor.execute('SELECT user_id, id, amount, category FROM recurring WHERE day = ?', (day,))
    for user_id, rec_id, amount, category in cursor.fetchall():
        add_expense(user_id, amount, category, now, rec_id)
    
    # Daily reminders
    cursor.execute('SELECT user_id FROM users WHERE notifications = TRUE')
    for (user_id,) in cursor.fetchall():
        await bot.send_message(user_id, "Не забудьте добавить сегодняшние траты!")

async def weekly_reports():
    cursor.execute('SELECT user_id FROM users')
    for (user_id,) in cursor.fetchall():
        report = format_report(user_id, 'week')
        await bot.send_message(user_id, report)

scheduler.add_job(daily_tasks, CronTrigger(hour=9, minute=0))  # 9:00 daily
scheduler.add_job(weekly_reports, CronTrigger(day_of_week='mon', hour=9, minute=0))  # Monday 9:00

# Handlers
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    user_id = message.from_user.id
    income = get_user_income(user_id)
    if income == 0:
        await message.reply("Привет! Я финансовый помощник. Я помогаю учитывать и оптимизировать траты.\nПожалуйста, введите ваш ежемесячный доход (например, 10000):")
        await IncomeState.income.set()
    else:
        await show_menu(message)

@dp.message_handler(state=IncomeState.income)
async def process_income(message: types.Message, state: FSMContext):
    try:
        income = float(message.text.replace(' ', ''))
        set_user_income(message.from_user.id, income)
        await state.finish()
        await show_menu(message)
    except ValueError:
        await message.reply("Неверный формат. Введите число, например 10000.")

async def show_menu(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ["Добавить трату", "История", "Моя статистика", "Помощь"]
    keyboard.add(*buttons)
    await message.reply("Выберите действие:", reply_markup=keyboard)

@dp.message_handler(Text(equals="Добавить трату"))
async def add_expense_start(message: types.Message):
    await message.reply("Введите сумму траты:")
    await ExpenseState.amount.set()

@dp.message_handler(state=ExpenseState.amount)
async def process_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.replace(' ', ''))
        await state.update_data(amount=amount)
        keyboard = InlineKeyboardMarkup(row_width=2)
        for cat in ALL_CATEGORIES:
            keyboard.add(InlineKeyboardButton(cat, callback_data=f"cat_{cat}"))
        await message.reply("Выберите категорию:", reply_markup=keyboard)
        await ExpenseState.category.set()
    except ValueError:
        await message.reply("Неверный формат. Введите число.")

@dp.callback_query_handler(lambda c: c.data.startswith('cat_'), state=ExpenseState.category)
async def process_category(callback: CallbackQuery, state: FSMContext):
    category = callback.data.split('_')[1]
    data = await state.get_data()
    amount = data['amount']
    user_id = callback.from_user.id
    warnings = check_and_notify_limits(user_id, category, amount)
    add_expense(user_id, amount, category)
    await bot.answer_callback_query(callback.id)
    await bot.send_message(user_id, f"Трата добавлена: {amount:.0f} ₽ на {category}")
    if warnings:
        await bot.send_message(user_id, "\n".join(warnings))
    await state.finish()
    await show_menu(types.Message(chat=types.Chat(id=user_id), from_user=types.User(id=user_id)))

@dp.message_handler(lambda message: message.text.replace(' ', '').isdigit(), state='*')
async def quick_add_amount(message: types.Message):
    await process_amount(message, FSMContext(dp.storage, message.chat.id, message.from_user.id))

@dp.message_handler(Text(equals="История"))
async def history(message: types.Message):
    user_id = message.from_user.id
    expenses = get_expenses(user_id)
    if not expenses:
        await message.reply("Нет трат.")
        return
    for exp_id, amount, category, ts in expenses:
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("❌", callback_data=f"del_{exp_id}"))
        await message.reply(f"{ts}: {amount:.0f} ₽ - {category}", reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data.startswith('del_'))
async def delete_callback(callback: CallbackQuery):
    exp_id = int(callback.data.split('_')[1])
    delete_expense(exp_id)
    await bot.answer_callback_query(callback.id, "Трата удалена")
    await bot.delete_message(callback.message.chat.id, callback.message.message_id)

@dp.message_handler(Text(equals="Моя статистика"))
async def statistics(message: types.Message):
    text = format_statistics(message.from_user.id)
    await message.reply(text)

@dp.message_handler(Text(equals="Помощь"))
async def help_command(message: types.Message):
    text = """
Команды:
/start - начать
/add_recurring - добавить регулярную трату (сумма, категория, день месяца)
/toggle_notifications - включить/выключить уведомления
/report week - отчет за неделю
/report month - отчет за месяц
"""
    await message.reply(text)

@dp.message_handler(commands=['add_recurring'])
async def add_recurring_start(message: types.Message):
    await message.reply("Введите сумму:")
    await RecurringState.amount.set()

@dp.message_handler(state=RecurringState.amount)
async def recurring_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.replace(' ', ''))
        await state.update_data(amount=amount)
        keyboard = InlineKeyboardMarkup(row_width=2)
        for cat in ALL_CATEGORIES:
            keyboard.add(InlineKeyboardButton(cat, callback_data=f"rec_cat_{cat}"))
        await message.reply("Выберите категорию:", reply_markup=keyboard)
        await RecurringState.category.set()
    except ValueError:
        await message.reply("Неверный формат.")

@dp.callback_query_handler(lambda c: c.data.startswith('rec_cat_'), state=RecurringState.category)
async def recurring_category(callback: CallbackQuery, state: FSMContext):
    category = callback.data.split('_')[2]
    await state.update_data(category=category)
    await bot.answer_callback_query(callback.id)
    await bot.send_message(callback.from_user.id, "Введите день месяца (1-31):")
    await RecurringState.day.set()

@dp.message_handler(state=RecurringState.day)
async def recurring_day(message: types.Message, state: FSMContext):
    try:
        day = int(message.text)
        if not 1 <= day <= 31:
            raise ValueError
        data = await state.get_data()
        add_recurring(message.from_user.id, data['amount'], data['category'], day)
        await message.reply("Регулярная трата добавлена.")
        await state.finish()
    except ValueError:
        await message.reply("Неверный день.")

@dp.message_handler(commands=['toggle_notifications'])
async def toggle_notif(message: types.Message):
    new = toggle_notifications(message.from_user.id)
    await message.reply(f"Уведомления {'включены' if new else 'выключены'}.")

@dp.message_handler(commands=['report'])
async def report(message: types.Message):
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Использование: /report week или /report month")
        return
    period = args[1]
    text = format_report(message.from_user.id, period)
    await message.reply(text)

# Start scheduler
scheduler.start()

# Webhook setup for Render
async def on_startup(dp):
    webhook_url = os.environ.get('WEBHOOK_URL')
    if webhook_url:
        await bot.set_webhook(webhook_url)

if __name__ == '__main__':
    executor.start_webhook(
        dispatcher=dp,
        webhook_path='/',
        on_startup=on_startup,
        skip_updates=True,
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 8000))
    )
