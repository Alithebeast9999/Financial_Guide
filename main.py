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
PORT = int(os.getenv("PORT", "8080"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("financial_guide")

if not BOT_TOKEN:
    logger.critical("BOT_TOKEN не найден в переменных окружения. Установи BOT_TOKEN на Render.")
    raise SystemExit("BOT_TOKEN not found")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ---------- HEALTH SERVER ----------
app = Flask("financial_guide_health")

@app.route("/")
def health():
    return "Financial Guide is running", 200

def run_health():
    try:
        app.run(host="0.0.0.0", port=PORT)
    except Exception as e:
        logger.warning("Health server stopped: %s", e)

Thread(target=run_health, daemon=True).start()
logger.info(f"Health endpoint started on port {PORT}")

# ---------- SQLITE ----------
DB_PATH = os.getenv("DB_PATH", "data.sqlite")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    # Таблица расходов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            category TEXT NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL
        )
    """)

    # Таблица категорий
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    """)

    # Добавим базовые категории, если их нет
    base_categories = ["еда", "транспорт", "жильё", "развлечения", "здоровье", "одежда", "прочее"]
    for cat in base_categories:
        cur.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (cat,))

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

def get_recent_expenses(user_id, limit=10):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT amount, category, note, created_at
        FROM expenses
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT ?
    """, (user_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows

def delete_last_expense(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id FROM expenses WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", (user_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False
    cur.execute("DELETE FROM expenses WHERE id = ?", (row["id"],))
    conn.commit()
    conn.close()
    return True

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
    types.KeyboardButton("🕒 История"),
    types.KeyboardButton("↩️ Отменить последнюю"),
)
main_menu.add(
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
        f"👋 Привет, <b>{first}</b>!\n\n"
        "Я — <b>Financial Guide</b> 💼\n"
        "Помогу вести учёт расходов и анализировать траты.\n\n"
        "Отправь сообщение в формате <code>500 еда</code> или выбери действие ниже 👇"
    )
    bot.send_message(message.chat.id, text, reply_markup=main_menu)

@bot.message_handler(commands=['help'])
def cmd_help(message):
    bot.send_message(message.chat.id,
        "ℹ️ Формат добавления: <code>500 еда</code>\n"
        "/history — последние расходы\n"
        "/undo — удалить последнюю запись\n"
        "/summary — сумма по категориям"
    )

@bot.message_handler(commands=['history'])
def cmd_history(message):
    rows = get_recent_expenses(message.from_user.id)
    if not rows:
        bot.send_message(message.chat.id, "Пока нет расходов.")
        return
    text = "🕒 Последние траты:\n\n"
    for r in rows:
        dt = datetime.fromisoformat(r["created_at"]).strftime("%d.%m %H:%M")
        note = f" ({r['note']})" if r["note"] else ""
        text += f"• {dt} — {r['category']}: {r['amount']:.2f} ₽{note}\n"
    bot.send_message(message.chat.id, text)

@bot.message_handler(commands=['undo'])
def cmd_undo(message):
    ok = delete_last_expense(message.from_user.id)
    bot.send_message(message.chat.id, "✅ Последняя запись удалена." if ok else "❌ Нет записей для удаления.")

@bot.message_handler(commands=['summary'])
def cmd_summary(message):
    now = datetime.utcnow()
    rows = get_month_stats(message.from_user.id, now.year, now.month)
    if not rows:
        bot.send_message(message.chat.id, "Нет данных за этот месяц.")
        return
    total = sum(r["total"] for r in rows)
    text = f"📊 Расходы за {now.strftime('%B %Y')}:\n\n"
    for r in rows:
        perc = (r["total"] / total) * 100
        text += f"{r['category']}: {r['total']:.2f} ₽ ({perc:.1f}%)\n"
    text += f"\n💵 Всего: {total:.2f} ₽"
    bot.send_message(message.chat.id, text)

@bot.message_handler(func=lambda m: True)
def all_messages(message):
    txt = (message.text or "").strip()
    parsed = parse_expense_text(txt)
    if parsed:
        amount, category = parsed
        add_expense(message.from_user.id, amount, category)
        bot.send_message(message.chat.id, f"✅ Записано: {amount:.2f} ₽ — {category}", reply_markup=main_menu)
    else:
        bot.send_message(message.chat.id, "❓ Не понял. Введи, например: <code>200 еда</code>", reply_markup=main_menu)

# ---------- START ----------
def start_polling():
    try:
        logger.info("Удаление webhook...")
        bot.remove_webhook()
        logger.info("Polling...")
        bot.polling(none_stop=True, interval=0, timeout=20)
    except Exception as e:
        logger.exception("Polling error: %s", e)
        raise

if __name__ == "__main__":
    logger.info("Financial Guide запущен")
    start_polling()
