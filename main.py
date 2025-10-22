# main.py
import os
import logging
from threading import Thread
from datetime import datetime
import sqlite3

import telebot
from telebot import types
from flask import Flask

# ---------- CONFIG ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "8080"))  # Render provides PORT env var

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("financial_guide")

if not BOT_TOKEN:
    logger.critical("BOT_TOKEN не найден в переменных окружения. Установи BOT_TOKEN на Render.")
    raise SystemExit("BOT_TOKEN not found")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ---------- HEALTH SERVER (для Render Web Service) ----------
app = Flask("financial_guide_health")

@app.route("/")
def health():
    return "Financial Guide is running", 200

def run_health():
    # bind to 0.0.0.0 so platform can access it
    try:
        app.run(host="0.0.0.0", port=PORT)
    except Exception as e:
        logger.warning("Health server stopped: %s", e)

Thread(target=run_health, daemon=True).start()
logger.info(f"Health endpoint started on port {PORT}")

# ---------- SIMPLE SQLITE STORAGE ----------
DB_PATH = os.getenv("DB_PATH", "data.sqlite")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS expenses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        category TEXT NOT NULL,
        note TEXT,
        created_at TEXT NOT NULL
    )""")
    conn.commit()
    conn.close()

def add_expense(user_id, amount, category, note=""):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO expenses (user_id, amount, category, note, created_at) VALUES (?, ?, ?, ?, ?)",
        (user_id, amount, category, note, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()

def get_month_stats(user_id, year, month):
    conn = get_db_connection()
    cur = conn.cursor()
    start = datetime(year, month, 1).isoformat()
    if month == 12:
        end = datetime(year + 1, 1, 1).isoformat()
    else:
        end = datetime(year, month + 1, 1).isoformat()
    cur.execute("""
        SELECT category, SUM(amount) as total, COUNT(*) as cnt
        FROM expenses
        WHERE user_id = ? AND created_at >= ? AND created_at < ?
        GROUP BY category
        ORDER BY total DESC
    """, (user_id, start, end))
    rows = cur.fetchall()
    conn.close()
    return rows

init_db()
logger.info("SQLite DB initialized (data.sqlite)")

# ---------- BOT UI ----------
main_menu = types.ReplyKeyboardMarkup(resize_keyboard=True)
main_menu.add(
    types.KeyboardButton("💰 Добавить трату"),
    types.KeyboardButton("📊 Моя статистика"),
    types.KeyboardButton("🎯 Цели (скоро)"),
    types.KeyboardButton("ℹ️ Помощь")
)

def parse_expense_text(text: str):
    if not text:
        return None
    t = text.strip().replace(",", ".")
    parts = t.split()
    try:
        amount = float(parts[0])
        category = " ".join(parts[1:]) if len(parts) > 1 else "прочее"
        return amount, category
    except Exception:
        # try to find numeric token anywhere
        for i, p in enumerate(parts):
            try:
                amount = float(p)
                category = " ".join(parts[:i] + parts[i+1:]) or "прочее"
                return amount, category
            except Exception:
                continue
    return None

@bot.message_handler(commands=['start'])
def cmd_start(message):
    first = message.from_user.first_name or "друг"
    text = (
        f"👋 Привет! Я — <b>Financial Guide</b>, помогу следить за расходами и целями.\n\n"
        "Отправь сообщение в формате <code>500 еда</code> чтобы быстро добавить трату.\n\n"
        "Или выбери действие в меню ниже."
    )
    bot.send_message(message.chat.id, text, reply_markup=main_menu)

@bot.message_handler(commands=['help'])
def cmd_help(message):
    bot.send_message(message.chat.id, "ℹ️ Отправь '500 еда' или используй кнопки меню. Команды: /start /help")

add_state = {}

@bot.message_handler(func=lambda m: m.text == "💰 Добавить трату")
def start_add_flow(message):
    add_state[message.from_user.id] = {"step": "ask_amount"}
    bot.send_message(message.chat.id, "Введите сумму и категорию. Пример: <code>250 кафе</code>")

@bot.message_handler(func=lambda m: m.text == "📊 Моя статистика")
def show_stats(message):
    now = datetime.utcnow()
    rows = get_month_stats(message.from_user.id, now.year, now.month)
    if not rows:
        bot.send_message(message.chat.id, "Пока нет расходов за этот месяц.")
    else:
        bot.send_message(message.chat.id, ("\n" + "\n").join([f"{r['category']}: {r['total']:.2f} ₽ ({r['cnt']} записей)" for r in rows]))

@bot.message_handler(func=lambda m: True)
def all_messages(message):
    user_id = message.from_user.id
    txt = (message.text or "").strip()
    state = add_state.get(user_id)
    if state and state.get("step") == "ask_amount":
        parsed = parse_expense_text(txt)
        if parsed:
            amount, category = parsed
            add_expense(user_id, amount, category)
            bot.send_message(message.chat.id, f"✅ Записано: {amount:.2f} ₽ — {category}", reply_markup=main_menu)
        else:
            bot.send_message(message.chat.id, "Не удалось распознать. Введи в формате: <code>500 еда</code>")
        add_state.pop(user_id, None)
        return

    parsed = parse_expense_text(txt)
    if parsed:
        amount, category = parsed
        add_expense(user_id, amount, category)
        bot.send_message(message.chat.id, f"✅ Быстрая запись: {amount:.2f} ₽ — {category}", reply_markup=main_menu)
        return

    bot.send_message(message.chat.id, "❓ Не понял сообщение. Используй меню или отправь сумму и категорию.", reply_markup=main_menu)

# ---------- START POLLING (удаляем возможный webhook) ----------
def start_polling():
    try:
        logger.info("Попытка удалить webhook (если был установлен)...")
        try:
            bot.remove_webhook()
        except Exception as e:
            logger.debug("remove_webhook() raised: %s", e)
        logger.info("Запуск polling...")
        bot.polling(none_stop=True, interval=0, timeout=20)
    except Exception as e:
        logger.exception("Polling остановлен с исключением: %s", e)
        raise

if __name__ == "__main__":
    logger.info("Financial Guide стартует")
    start_polling()
