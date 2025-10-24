import os
import sqlite3
import telebot
from flask import Flask, request

BOT_TOKEN = os.getenv("BOT_TOKEN")
bot = telebot.TeleBot(BOT_TOKEN)

app = Flask(__name__)

DB_PATH = "finance.db"

# -----------------------
# Инициализация базы
# -----------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Таблица пользователей
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            user_id INTEGER UNIQUE,
            budget REAL DEFAULT 0
        )
    ''')
    # Таблица расходов
    c.execute('''
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            category TEXT,
            amount REAL,
            date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

# -----------------------
# Хелперы для базы
# -----------------------
def get_budget(user_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT budget FROM users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def set_budget(user_id, amount):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (user_id, budget) VALUES (?, ?)", (user_id, amount))
    c.execute("UPDATE users SET budget=? WHERE user_id=?", (amount, user_id))
    conn.commit()
    conn.close()

def add_expense(user_id, category, amount):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO expenses (user_id, category, amount) VALUES (?, ?, ?)", (user_id, category, amount))
    conn.commit()
    conn.close()

def get_total_spent(user_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT SUM(amount) FROM expenses WHERE user_id=?", (user_id,))
    total = c.fetchone()[0]
    conn.close()
    return total if total else 0

# -----------------------
# Команды
# -----------------------
@bot.message_handler(commands=['start'])
def start(message):
    markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add("💰 Добавить трату", "📊 Моя статистика")
    markup.add("💳 Установить бюджет", "↩️ Отменить последнюю")
    markup.add("📤 Экспорт CSV")

    bot.send_message(
        message.chat.id,
        "👋 Привет! Я твой финансовый помощник.\n\n"
        "Можешь просто написать: `250 еда` или выбрать действие из меню ниже.",
        reply_markup=markup
    )

# -----------------------
# Установка бюджета
# -----------------------
@bot.message_handler(func=lambda msg: msg.text == "💳 Установить бюджет")
def ask_budget(message):
    bot.send_message(message.chat.id, "Введите сумму вашего месячного бюджета (в $):")
    bot.register_next_step_handler(message, save_budget)

def save_budget(message):
    try:
        amount = float(message.text)
        set_budget(message.from_user.id, amount)
        bot.send_message(message.chat.id, f"✅ Месячный бюджет установлен: {amount:.2f} $")
    except ValueError:
        bot.send_message(message.chat.id, "❌ Введите число, например: 1500")

# -----------------------
# Добавление траты
# -----------------------
@bot.message_handler(func=lambda msg: msg.text == "💰 Добавить трату")
def ask_expense(message):
    bot.send_message(message.chat.id, "Введите трату в формате: `сумма категория` (например: `250 еда`)")
    bot.register_next_step_handler(message, handle_expense)

@bot.message_handler(func=lambda msg: True)
def handle_expense(message):
    parts = message.text.strip().split(" ", 1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "❌ Формат неверный. Пример: `250 еда`")
        return

    try:
        amount = float(parts[0])
        category = parts[1]
        add_expense(message.from_user.id, category, amount)
        total_spent = get_total_spent(message.from_user.id)
        budget = get_budget(message.from_user.id)

        msg = f"✅ Добавлено: {amount:.2f}$ на {category}.\nОбщий расход: {total_spent:.2f}$."
        if budget > 0 and total_spent > budget:
            msg += f"\n⚠️ Вы превысили месячный бюджет!\nВаши расходы: {total_spent:.2f} / {budget:.2f}$."

        bot.send_message(message.chat.id, msg)
    except ValueError:
        bot.send_message(message.chat.id, "❌ Неверная сумма. Пример: `250 еда`")

# -----------------------
# Flask health-check
# -----------------------
@app.route('/')
def index():
    return "Bot is running"

if __name__ == "__main__":
    init_db()
    from threading import Thread
    Thread(target=lambda: bot.polling(none_stop=True, interval=0, timeout=20)).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
