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
    logger.critical("BOT_TOKEN не найден. Установи BOT_TOKEN на Render.")
    raise SystemExit("BOT_TOKEN not found")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ---------- HEALTH SERVER ----------
app = Flask("financial_guide")

@app.route("/")
def health():
    return "Financial Guide is running 🟢", 200

Thread(target=lambda: app.run(host="0.0.0.0", port=PORT), daemon=True).start()

# ---------- SQLITE ----------
DB_PATH = os.getenv("DB_PATH", "data.sqlite")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            category TEXT NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
    """)

    base_cats = ["еда", "транспорт", "жильё", "развлечения", "здоровье", "одежда", "прочее"]
    cur.executemany("INSERT OR IGNORE INTO categories (name) VALUES (?)", [(c,) for c in base_cats])
    conn.commit()
    conn.close()

def add_expense(user_id, amount, category, note=""):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO expenses (user_id, amount, category, note, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, amount, category, note, datetime.utcnow().isoformat())
        )

def get_recent_expenses(user_id, limit=10):
    with get_db_connection() as conn:
        return conn.execute(
            "SELECT amount, category, note, created_at FROM expenses WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        ).fetchall()

def delete_last_expense(user_id):
    with get_db_connection() as conn:
        row = conn.execute("SELECT id FROM expenses WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", (user_id,)).fetchone()
        if not row:
            return False
        conn.execute("DELETE FROM expenses WHERE id = ?", (row["id"],))
        return True

def get_month_stats(user_id, year, month):
    start = datetime(year, month, 1).isoformat()
    end = datetime(year + (month // 12), (month % 12) + 1, 1).isoformat()
    with get_db_connection() as conn:
        return conn.execute("""
            SELECT category, SUM(amount) as total, COUNT(*) as cnt
            FROM expenses
            WHERE user_id = ? AND created_at >= ? AND created_at < ?
            GROUP BY category ORDER BY total DESC
        """, (user_id, start, end)).fetchall()

init_db()
logger.info("SQLite DB initialized")

# ---------- BOT MENUS ----------
main_menu = types.ReplyKeyboardMarkup(resize_keyboard=True)
main_menu.add(
    types.KeyboardButton("💰 Добавить трату"),
    types.KeyboardButton("📊 Моя статистика"),
    types.KeyboardButton("🕒 История")
)
main_menu.add(
    types.KeyboardButton("↩️ Отменить последнюю"),
    types.KeyboardButton("ℹ️ Помощь")
)

def inline_main_menu():
    markup = types.InlineKeyboardMarkup()
    markup.row(types.InlineKeyboardButton("💰 Анализ расходов", callback_data="expenses"))
    markup.row(types.InlineKeyboardButton("📊 Прогноз и статистика", callback_data="forecast"))
    markup.row(types.InlineKeyboardButton("🧾 Управление бюджетом", callback_data="budget"))
    markup.row(types.InlineKeyboardButton("⚙️ Настройки", callback_data="settings"))
    return markup

# ---------- PARSING ----------
def parse_expense_text(text: str):
    if not text:
        return None
    t = text.strip().replace(",", ".")
    parts = t.split()
    for i, p in enumerate(parts):
        try:
            amount = float(p)
            category = " ".join(parts[:i] + parts[i+1:]) or "прочее"
            return amount, category
        except ValueError:
            continue
    return None

# ---------- BOT HANDLERS ----------
@bot.message_handler(commands=['start'])
def cmd_start(message):
    first = message.from_user.first_name or "друг"
    text = (
        f"👋 Привет, <b>{first}</b>!\n\n"
        "Я — <b>Financial Guide</b> 💼\n"
        "Помогу вести учёт расходов, анализировать траты и прогнозировать бюджет.\n\n"
        "Можешь отправить сумму и категорию (например: <code>250 кофе</code>), "
        "или использовать меню ниже 👇"
    )
    bot.send_message(message.chat.id, text, reply_markup=main_menu)
    bot.send_message(message.chat.id, "📋 Или воспользуйся интерактивным меню:", reply_markup=inline_main_menu())

@bot.callback_query_handler(func=lambda call: True)
def inline_handler(call):
    data = call.data
    if data == "expenses":
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "💰 Анализ расходов пока в разработке.")
    elif data == "forecast":
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "📊 Раздел прогнозов будет добавлен позже.")
    elif data == "budget":
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "🧾 Управление бюджетом скоро появится.")
    elif data == "settings":
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "⚙️ Здесь появятся настройки профиля.")
    else:
        bot.answer_callback_query(call.id, "Неизвестная команда.")

@bot.message_handler(commands=['help'])
def cmd_help(message):
    bot.send_message(message.chat.id,
        "ℹ️ Команды:\n"
        "/history — последние расходы\n"
        "/undo — удалить последнюю запись\n"
        "/summary — сумма по категориям\n\n"
        "💡 Или просто напиши сообщение вроде: <code>350 транспорт</code>")

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
def handle_expense(message):
    parsed = parse_expense_text(message.text or "")
    if parsed:
        amount, category = parsed
        add_expense(message.from_user.id, amount, category)
        bot.send_message(message.chat.id, f"✅ Записано: {amount:.2f} ₽ — {category}", reply_markup=main_menu)
    else:
        bot.send_message(message.chat.id, "❓ Не понял. Введи, например: <code>200 еда</code>", reply_markup=main_menu)

# ---------- START ----------
if __name__ == "__main__":
    logger.info("Financial Guide запущен ✅")
    bot.remove_webhook()
    bot.polling(none_stop=True, interval=0, timeout=20)

