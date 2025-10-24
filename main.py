# main.py
import os
import logging
from threading import Thread
from datetime import datetime
import sqlite3
import io
import csv

import telebot
from telebot import types
from flask import Flask

# ---------- CONFIG ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "8080"))
DB_PATH = os.getenv("DB_PATH", "data.sqlite")
CURRENCY = os.getenv("CURRENCY", "₽")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("financial_guide")

if not BOT_TOKEN:
    logger.critical("BOT_TOKEN не найден. Установи BOT_TOKEN в переменных окружения.")
    raise SystemExit("BOT_TOKEN not found")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ---------- HEALTH SERVER ----------
app = Flask("financial_guide_health")

@app.route("/")
def health():
    return "Financial Guide is running 🟢", 200

Thread(target=lambda: app.run(host="0.0.0.0", port=PORT), daemon=True).start()
logger.info(f"Health endpoint started on port {PORT}")

# ---------- DATABASE HELPERS ----------
def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db_connection() as conn:
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
            CREATE TABLE IF NOT EXISTS budgets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                amount REAL NOT NULL,
                created_at TEXT NOT NULL
            );
        """)
        base_cats = ["еда", "транспорт", "жильё", "развлечения", "здоровье", "одежда", "прочее"]
        cur.executemany("INSERT OR IGNORE INTO categories (name) VALUES (?)", [(c,) for c in base_cats])
        conn.commit()

def add_expense(user_id: int, amount: float, category: str, note: str = ""):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO expenses (user_id, amount, category, note, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, amount, category, note, datetime.utcnow().isoformat())
        )

def get_recent_expenses(user_id: int, limit: int = 10):
    with get_db_connection() as conn:
        return conn.execute(
            "SELECT id, amount, category, note, created_at FROM expenses WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        ).fetchall()

def delete_last_expense(user_id: int):
    with get_db_connection() as conn:
        row = conn.execute("SELECT id FROM expenses WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", (user_id,)).fetchone()
        if not row:
            return False
        conn.execute("DELETE FROM expenses WHERE id = ?", (row["id"],))
        return True

def get_month_stats(user_id: int, year: int, month: int):
    start = datetime(year, month, 1).isoformat()
    if month == 12:
        end = datetime(year + 1, 1, 1).isoformat()
    else:
        end = datetime(year, month + 1, 1).isoformat()
    with get_db_connection() as conn:
        return conn.execute("""
            SELECT category, SUM(amount) as total, COUNT(*) as cnt
            FROM expenses
            WHERE user_id = ? AND created_at >= ? AND created_at < ?
            GROUP BY category
            ORDER BY total DESC
        """, (user_id, start, end)).fetchall()

def set_budget(user_id: int, year: int, month: int, amount: float):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO budgets (user_id, year, month, amount, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, year, month, amount, datetime.utcnow().isoformat())
        )

def get_budget(user_id: int, year: int, month: int):
    with get_db_connection() as conn:
        row = conn.execute("SELECT amount FROM budgets WHERE user_id = ? AND year = ? AND month = ? ORDER BY created_at DESC LIMIT 1",
                           (user_id, year, month)).fetchone()
        return row["amount"] if row else None

def export_csv(user_id: int, year: int, month: int):
    # returns BytesIO object with CSV
    start = datetime(year, month, 1).isoformat()
    if month == 12:
        end = datetime(year + 1, 1, 1).isoformat()
    else:
        end = datetime(year, month + 1, 1).isoformat()
    with get_db_connection() as conn:
        rows = conn.execute("SELECT created_at, category, amount, note FROM expenses WHERE user_id = ? AND created_at >= ? AND created_at < ? ORDER BY created_at",
                            (user_id, start, end)).fetchall()
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["created_at", "category", "amount", "note"])
    for r in rows:
        writer.writerow([r["created_at"], r["category"], f"{r['amount']:.2f}", r["note"] or ""])
    bio = io.BytesIO(buf.getvalue().encode("utf-8"))
    bio.seek(0)
    return bio

init_db()
logger.info("DB initialized")

# ---------- UI: ReplyKeyboard (single source of quick actions) ----------
main_menu = types.ReplyKeyboardMarkup(resize_keyboard=True)
main_menu.add(types.KeyboardButton("💰 Добавить трату"))
main_menu.add(types.KeyboardButton("📊 Моя статистика"), types.KeyboardButton("🕒 История"))
main_menu.add(types.KeyboardButton("↩️ Отменить последнюю"), types.KeyboardButton("📤 Экспорт CSV"))
main_menu.add(types.KeyboardButton("💳 Установить бюджет"), types.KeyboardButton("ℹ️ Помощь"))

# Temporary per-user state for guided flows (in-memory)
# structure example: add_state[user_id] = {"step": "amount"/"category"/"note", "amount": 100.0, "category": "еда"}
add_state = {}
budget_state = {}

# ---------- HELPERS ----------
def parse_expense_text(text: str):
    """Try parse message as expense: finds first numeric token as amount and rest as category."""
    if not text:
        return None
    t = text.strip().replace(",", ".")
    parts = t.split()
    for i, token in enumerate(parts):
        try:
            amount = float(token)
            category = " ".join(parts[:i] + parts[i+1:]) or "прочее"
            return amount, category
        except ValueError:
            continue
    return None

def fmt_money(x):
    return f"{x:.2f} {CURRENCY}"

# ---------- BOT HANDLERS ----------
@bot.message_handler(commands=['start'])
def cmd_start(message):
    first = message.from_user.first_name or "друг"
    bot.send_message(
        message.chat.id,
        f"👋 Привет, <b>{first}</b>!\n\nЯ — <b>Financial Guide</b>.\n"
        "Можно быстро записывать траты текстом (пример: <code>250 еда</code>) или через кнопку ниже.",
        reply_markup=main_menu
    )

@bot.message_handler(commands=['help'])
def cmd_help(message):
    bot.send_message(message.chat.id,
        "ℹ️ Как пользоваться:\n"
        "- Быстрая запись: пришли сообщение '500 еда'\n"
        "- Кнопка '💰 Добавить трату' запускает пошаговый ввод\n"
        "- '📊 Моя статистика' — суммарно по категориям за текущий месяц\n"
        "- '🕒 История' — последние записи\n"
        "- '↩️ Отменить последнюю' — удалить последнюю запись (с подтверждением)\n"
        "- '📤 Экспорт CSV' — получить CSV за текущий месяц\n"
        "- '💳 Установить бюджет' — задать месячный бюджет"
    )

# --- Guided add flow via ReplyKeyboard (step-by-step)
@bot.message_handler(func=lambda m: m.text == "💰 Добавить трату")
def start_add_flow(message):
    add_state[message.from_user.id] = {"step": "ask_amount"}
    bot.send_message(message.chat.id, "Введите сумму (например: 250). Можно с категорией: '250 кафе'")

@bot.message_handler(func=lambda m: add_state.get(m.from_user.id, {}).get("step") == "ask_amount")
def handle_add_amount(message):
    user_id = message.from_user.id
    txt = (message.text or "").strip()
    parsed = parse_expense_text(txt)
    if parsed:
        amount, category = parsed
        add_state[user_id] = {"step": "confirm", "amount": float(amount), "category": category, "note": ""}
        bot.send_message(user_id, f"Добавляем: {fmt_money(amount)} — <b>{category}</b>. Напиши заметку или 'OK' для подтверждения.")
        return
    # try parse pure float
    try:
        amount = float(txt.replace(",", "."))
        add_state[user_id] = {"step": "ask_category", "amount": amount}
        bot.send_message(user_id, "Введите категорию (например: еда). Можно нажать 'прочее' просто отправив слово.")
    except ValueError:
        bot.send_message(user_id, "Не удалось распознать сумму. Введите число (например: 250) или '250 еда'.")

@bot.message_handler(func=lambda m: add_state.get(m.from_user.id, {}).get("step") == "ask_category")
def handle_add_category(message):
    user_id = message.from_user.id
    state = add_state.get(user_id)
    if not state:
        bot.send_message(user_id, "Что-то пошло не так. Начни /start или '💰 Добавить трату'.")
        return
    category = (message.text or "прочее").strip()
    state.update({"step": "confirm", "category": category, "note": ""})
    bot.send_message(user_id, f"Добавляем: {fmt_money(state['amount'])} — <b>{category}</b>. Напиши заметку или 'OK' для подтверждения.")

@bot.message_handler(func=lambda m: add_state.get(m.from_user.id, {}).get("step") == "confirm")
def handle_add_confirm(message):
    user_id = message.from_user.id
    state = add_state.get(user_id)
    if not state:
        bot.send_message(user_id, "Сессия времени жизни закончилась. Нажмите '💰 Добавить трату' для новой записи.")
        return
    text = (message.text or "").strip()
    if text.lower() in ("ok", "готово", "готово."):
        add_expense(user_id, float(state["amount"]), state["category"], state.get("note", ""))
        bot.send_message(user_id, f"✅ Записано: {fmt_money(state['amount'])} — {state['category']}", reply_markup=main_menu)
        add_state.pop(user_id, None)
        return
    # otherwise treat input as note and then save
    note = text
    add_expense(user_id, float(state["amount"]), state["category"], note)
    bot.send_message(user_id, f"✅ Записано: {fmt_money(state['amount'])} — {state['category']} ({note})", reply_markup=main_menu)
    add_state.pop(user_id, None)

# --- Quick parse from free text (fast path)
@bot.message_handler(func=lambda m: True)
def handle_text(message):
    # Ignore if user in guided flow (handlers above capture those)
    if add_state.get(message.from_user.id):
        return  # guided handlers will process
    txt = (message.text or "").strip()
    # Quick commands via keyboard handled separately; here parse free-form expense
    parsed = parse_expense_text(txt)
    if parsed:
        amount, category = parsed
        add_expense(message.from_user.id, amount, category)
        bot.send_message(message.chat.id, f"✅ Быстрая запись: {fmt_money(amount)} — {category}", reply_markup=main_menu)
        return
    # Handle other quick buttons:
    if txt == "📊 Моя статистика":
        now = datetime.utcnow()
        rows = get_month_stats(message.from_user.id, now.year, now.month)
        if not rows:
            bot.send_message(message.chat.id, "Нет данных за текущий месяц.", reply_markup=main_menu)
            return
        total = sum(r["total"] for r in rows)
        text = f"📊 Расходы за {now.strftime('%B %Y')}:\n\n"
        for r in rows:
            perc = (r["total"] / total) * 100 if total else 0
            text += f"{r['category']}: {fmt_money(r['total'])} ({perc:.1f}%)\n"
        text += f"\n💵 Всего: {fmt_money(total)}"
        bot.send_message(message.chat.id, text, reply_markup=main_menu)
        return
    if txt == "🕒 История":
        rows = get_recent_expenses(message.from_user.id, limit=10)
        if not rows:
            bot.send_message(message.chat.id, "Пока нет расходов.", reply_markup=main_menu)
            return
        out = "🕒 Последние траты:\n\n"
        for r in rows:
            dt = datetime.fromisoformat(r["created_at"]).strftime("%d.%m %H:%M")
            note = f" ({r['note']})" if r["note"] else ""
            out += f"• {dt} — {r['category']}: {fmt_money(r['amount'])}{note}\n"
        bot.send_message(message.chat.id, out, reply_markup=main_menu)
        return
    if txt == "↩️ Отменить последнюю":
        # send confirmation inline
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("✅ Удалить", callback_data="confirm_undo"))
        markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
        bot.send_message(message.chat.id, "Вы уверены, что хотите удалить последнюю запись?", reply_markup=markup)
        return
    if txt == "📤 Экспорт CSV":
        now = datetime.utcnow()
        bio = export_csv(message.from_user.id, now.year, now.month)
        if bio.getbuffer().nbytes == 0:
            bot.send_message(message.chat.id, "Нет данных для экспорта.", reply_markup=main_menu)
            return
        bio.name = f"expenses_{now.year}_{now.month}.csv"
        bot.send_document(message.chat.id, bio)
        return
    if txt == "💳 Установить бюджет":
        budget_state[message.from_user.id] = {"step": "ask_budget"}
        bot.send_message(message.chat.id, "Введите месячный бюджет (числом), например: 50000")
        return
    if txt == "ℹ️ Помощь":
        cmd_help(message)
        return
    # if still nothing - suggest commands
    bot.send_message(message.chat.id, "❓ Не понял. Используйте меню или отправьте '250 еда' для быстрой записи.", reply_markup=main_menu)

# --- Inline callback handler (confirmations and quick actions)
@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    data = call.data
    if data == "confirm_undo":
        ok = delete_last_expense(call.from_user.id)
        bot.edit_message_text("✅ Последняя запись удалена." if ok else "❌ Нет записей для удаления.",
                              call.message.chat.id, call.message.message_id, reply_markup=None)
    elif data == "cancel":
        bot.edit_message_text("Операция отменена.", call.message.chat.id, call.message.message_id, reply_markup=None)
    else:
        bot.answer_callback_query(call.id, "Команда пока не реализована")

# --- Budget flow
@bot.message_handler(func=lambda m: budget_state.get(m.from_user.id, {}).get("step") == "ask_budget")
def handle_budget_amount(message):
    user_id = message.from_user.id
    txt = (message.text or "").strip()
    try:
        amount = float(txt.replace(",", "."))
        now = datetime.utcnow()
        set_budget(user_id, now.year, now.month, amount)
        bot.send_message(user_id, f"✅ Бюджет на {now.strftime('%B %Y')} установлен: {fmt_money(amount)}", reply_markup=main_menu)
        budget_state.pop(user_id, None)
    except ValueError:
        bot.send_message(user_id, "Неверный формат. Введите число, например: 50000")

# ---------- START ----------
if __name__ == "__main__":
    logger.info("Financial Guide запущен ✅")
    # Ensure webhook off
    try:
        bot.remove_webhook()
    except Exception:
        pass
    bot.polling(none_stop=True, interval=0, timeout=20)
