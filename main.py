# main.py
import os
import logging
from threading import Thread
from datetime import datetime
import sqlite3
import telebot
from telebot import types
from flask import Flask

# --------------- CONFIG ---------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "8080"))
DB_PATH = os.getenv("DB_PATH", "data.sqlite")
CURRENCY = "₽"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("financial_guide")

if not BOT_TOKEN:
    logger.critical("BOT_TOKEN не найден в переменных окружения.")
    raise SystemExit("BOT_TOKEN not found")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# --------------- HEALTH (Render) ---------------
app = Flask("financial_guide_health")


@app.route("/")
def health():
    return "Financial Guide is running 🟢", 200


Thread(target=lambda: app.run(host="0.0.0.0", port=PORT), daemon=True).start()
logger.info("Health endpoint started on port %s", PORT)

# --------------- DATABASE HELPERS ---------------
def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                budget REAL DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                category TEXT NOT NULL,
                note TEXT,
                created_at TEXT NOT NULL
            );
        """)
        conn.commit()


def get_budget(user_id: int):
    with get_db_connection() as conn:
        row = conn.execute("SELECT budget FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return row["budget"] if row else None


def set_budget(user_id: int, amount: float):
    with get_db_connection() as conn:
        conn.execute("INSERT OR IGNORE INTO users (user_id, budget) VALUES (?, ?)", (user_id, amount))
        conn.execute("UPDATE users SET budget = ? WHERE user_id = ?", (amount, user_id))
        conn.commit()


def add_expense(user_id: int, amount: float, category: str, note: str = ""):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO expenses (user_id, amount, category, note, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, amount, category, note, datetime.utcnow().isoformat())
        )
        conn.commit()


def delete_expense_by_id(expense_id: int):
    with get_db_connection() as conn:
        conn.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))
        conn.commit()


def get_recent_expenses(user_id: int, limit: int = 20):
    with get_db_connection() as conn:
        rows = conn.execute(
            "SELECT id, amount, category, note, created_at FROM expenses WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        ).fetchall()
        return rows


def get_month_stats(user_id: int, year: int, month: int):
    start = datetime(year, month, 1).isoformat()
    if month == 12:
        end = datetime(year + 1, 1, 1).isoformat()
    else:
        end = datetime(year, month + 1, 1).isoformat()
    with get_db_connection() as conn:
        rows = conn.execute("""
            SELECT category, SUM(amount) as total, COUNT(*) as cnt
            FROM expenses
            WHERE user_id = ? AND created_at >= ? AND created_at < ?
            GROUP BY category
            ORDER BY total DESC
        """, (user_id, start, end)).fetchall()
        return rows


def get_month_total(user_id: int, year: int, month: int):
    start = datetime(year, month, 1).isoformat()
    if month == 12:
        end = datetime(year + 1, 1, 1).isoformat()
    else:
        end = datetime(year, month + 1, 1).isoformat()
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND created_at >= ? AND created_at < ?",
            (user_id, start, end)
        ).fetchone()
        return row["total"] or 0.0


init_db()
logger.info("DB initialized at %s", DB_PATH)

# --------------- UI: Reply Keyboard ---------------
main_menu = types.ReplyKeyboardMarkup(resize_keyboard=True)
main_menu.add(types.KeyboardButton("💰 Добавить трату"))
main_menu.add(types.KeyboardButton("📊 Моя статистика"), types.KeyboardButton("🕒 История"))
main_menu.add(types.KeyboardButton("💳 Установить бюджет"), types.KeyboardButton("ℹ️ Помощь"))

# in-memory states for guided flows
add_state = {}     # {user_id: {"step":..., "amount":..., "category":..., "note":...}}
budget_state = {}  # {user_id: {"step": "ask_budget"}}


# --------------- HELPERS ---------------
def parse_expense_text(text: str):
    """
    Попытка распарсить входящий текст как трату.
    Логика: найти первую числовую токен-сумму, остальное — категория.
    Примеры: "250 еда", "еда 250" (тоже найдёт), "250.50 кафе"
    """
    if not text:
        return None
    t = text.strip().replace(",", ".")
    parts = t.split()
    # искать первый числовой токен
    for i, token in enumerate(parts):
        try:
            amount = float(token)
            category = " ".join(parts[:i] + parts[i+1:]) or "прочее"
            return round(amount, 2), category
        except ValueError:
            continue
    return None


def fmt_money(x: float):
    return f"{x:.2f} {CURRENCY}"


# --------------- HANDLERS ---------------

@bot.message_handler(commands=['start'])
def cmd_start(message: types.Message):
    user_id = message.from_user.id
    first = message.from_user.first_name or "друг"

    # Check budget; if not set, prompt to set immediately
    budget = get_budget(user_id)
    greeting = (
        f"👋 Привет, <b>{first}</b>!\n\n"
        "Я — <b>Financial Guide</b> — твой персональный помощник по учёту расходов и планированию бюджета.\n\n"
        "Я помогу быстро записывать траты, показывать статистику по категориям и предупреждать, если ты превышаешь месячный лимит.\n\n"
    )

    if budget is None:
        # Ask for budget immediately
        bot.send_message(user_id, greeting + "Сначала установим месячный бюджет. Введи сумму в рублях (например: 50000):")
        budget_state[user_id] = {"step": "ask_budget"}
        return

    # normal flow
    bot.send_message(user_id, greeting + "Используй меню ниже или отправь сообщение вида: <code>250 еда</code> для быстрой записи.", reply_markup=main_menu)


@bot.message_handler(func=lambda m: m.text == "💳 Установить бюджет")
def ask_budget(message: types.Message):
    user_id = message.from_user.id
    bot.send_message(user_id, "Введите месячный бюджет в рублях (числом), например: 50000")
    budget_state[user_id] = {"step": "ask_budget"}


@bot.message_handler(func=lambda m: budget_state.get(m.from_user.id, {}).get("step") == "ask_budget")
def handle_budget_input(message: types.Message):
    user_id = message.from_user.id
    txt = (message.text or "").strip()
    try:
        amount = float(txt.replace(",", "."))
        set_budget(user_id, amount)
        bot.send_message(user_id, f"✅ Месячный бюджет установлен: {fmt_money(amount)}", reply_markup=main_menu)
        budget_state.pop(user_id, None)
    except ValueError:
        bot.send_message(user_id, "❌ Неверный формат. Введите число, например: 50000")


@bot.message_handler(func=lambda m: m.text == "💰 Добавить трату")
def start_add_flow(message: types.Message):
    user_id = message.from_user.id
    # ensure budget exists
    if get_budget(user_id) is None:
        bot.send_message(user_id, "Сначала установите месячный бюджет. Введите его в рублях.")
        budget_state[user_id] = {"step": "ask_budget"}
        return
    add_state[user_id] = {"step": "ask_amount"}
    bot.send_message(user_id, "Введите сумму и (опционально) категорию. Пример: <code>250 кафе</code>")


@bot.message_handler(func=lambda m: add_state.get(m.from_user.id, {}).get("step") == "ask_amount")
def handle_add_amount(message: types.Message):
    user_id = message.from_user.id
    txt = (message.text or "").strip()
    parsed = parse_expense_text(txt)
    if parsed:
        amount, category = parsed
        add_state[user_id] = {"step": "confirm", "amount": float(amount), "category": category, "note": ""}
        bot.send_message(user_id, f"Добавляем: {fmt_money(amount)} — <b>{category}</b>.\nНапиши примечание или отправь 'OK' для подтверждения.")
        return
    # try interpret as pure number
    try:
        amount = float(txt.replace(",", "."))
        add_state[user_id] = {"step": "ask_category", "amount": amount}
        bot.send_message(user_id, "Введите категорию (например: еда).")
    except ValueError:
        bot.send_message(user_id, "Не получилось распознать сумму. Введите число или '250 еда'.")


@bot.message_handler(func=lambda m: add_state.get(m.from_user.id, {}).get("step") == "ask_category")
def handle_add_category(message: types.Message):
    user_id = message.from_user.id
    state = add_state.get(user_id)
    if not state:
        bot.send_message(user_id, "Сессия истекла. Нажмите '💰 Добавить трату' чтобы начать снова.")
        return
    category = (message.text or "прочее").strip()
    state.update({"step": "confirm", "category": category, "note": ""})
    bot.send_message(user_id, f"Добавляем: {fmt_money(state['amount'])} — <b>{category}</b>. Напишите примечание или 'OK' для подтверждения.")


@bot.message_handler(func=lambda m: add_state.get(m.from_user.id, {}).get("step") == "confirm")
def handle_add_confirm(message: types.Message):
    user_id = message.from_user.id
    state = add_state.get(user_id)
    if not state:
        bot.send_message(user_id, "Сессия истекла. Нажмите '💰 Добавить трату' чтобы начать снова.")
        return
    text = (message.text or "").strip()
    if text.lower() in ("ok", "готово", "ok."):
        note = state.get("note", "")
    else:
        note = text
    amount = float(state["amount"])
    category = state["category"]
    add_expense(user_id, amount, category, note)
    add_state.pop(user_id, None)

    # Check budget after adding
    now = datetime.utcnow()
    total = get_month_total(user_id, now.year, now.month)
    budget = get_budget(user_id) or 0.0
    reply = f"✅ Записано: {fmt_money(amount)} — {category}"
    if note:
        reply += f" ({note})"
    reply += f"\n\n📊 Потрачено за {now.strftime('%B %Y')}: {fmt_money(total)}"
    if budget and total > budget:
        reply += f"\n\n⚠️ Внимание! Вы превысили месячный бюджет: {fmt_money(total)} / {fmt_money(budget)}"
    bot.send_message(user_id, reply, reply_markup=main_menu)


# Quick free-text parsing (fast path), but only if user not in guided add/budget flows
@bot.message_handler(func=lambda m: True)
def handle_text(message: types.Message):
    user_id = message.from_user.id
    if add_state.get(user_id) or budget_state.get(user_id):
        # guided flows handled by their handlers
        return

    txt = (message.text or "").strip()

    # Quick commands via keyboard
    if txt == "📊 Моя статистика":
        # Show monthly summary + link to "История (последние N)" with deletable items
        now = datetime.utcnow()
        stats = get_month_stats(user_id, now.year, now.month)
        if not stats:
            bot.send_message(user_id, "Нет данных за текущий месяц.", reply_markup=main_menu)
            return
        total = sum(r["total"] for r in stats)
        out = f"📊 Расходы за {now.strftime('%B %Y')}:\n\n"
        for r in stats:
            perc = (r["total"] / total) * 100 if total else 0
            out += f"• {r['category']}: {fmt_money(r['total'])} ({perc:.1f}%)\n"
        out += f"\n💵 Всего: {fmt_money(total)}"
        # after summary, suggest viewing detailed history
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🕒 Показать последние траты", callback_data="show_history"))
        bot.send_message(user_id, out, reply_markup=markup)
        return

    if txt == "🕒 История":
        send_history_with_delete_buttons(user_id)
        return

    if txt == "💳 Установить бюджет":
        ask_budget(message)
        return

    if txt == "ℹ️ Помощь":
        send_help(message)
        return

    # Try quick parse as expense
    parsed = parse_expense_text(txt)
    if parsed:
        if get_budget(user_id) is None:
            bot.send_message(user_id, "Перед записью трат установите месячный бюджет. Нажмите '💳 Установить бюджет'.")
            budget_state[user_id] = {"step": "ask_budget"}
            return
        amount, category = parsed
        add_expense(user_id, amount, category, "")
        now = datetime.utcnow()
        total = get_month_total(user_id, now.year, now.month)
        budget = get_budget(user_id) or 0.0
        reply = f"✅ Быстрая запись: {fmt_money(amount)} — {category}\n\n📊 Потрачено за {now.strftime('%B %Y')}: {fmt_money(total)}"
        if budget and total > budget:
            reply += f"\n\n⚠️ Вы превысили месячный бюджет: {fmt_money(total)} / {fmt_money(budget)}"
        bot.send_message(user_id, reply, reply_markup=main_menu)
        return

    # nothing matched
    bot.send_message(user_id, "❓ Не понял. Используйте меню или отправьте '250 еда' для быстрой записи.", reply_markup=main_menu)


# --------------- HISTORY & DELETE LOGIC ---------------

def send_history_with_delete_buttons(user_id: int, limit: int = 20):
    rows = get_recent_expenses(user_id, limit=limit)
    if not rows:
        bot.send_message(user_id, "Пока нет записей.", reply_markup=main_menu)
        return

    # Telegram message size limits: chunk lines to not exceed limit; we'll send up to limit items
    lines = []
    buttons = types.InlineKeyboardMarkup(row_width=1)
    for r in rows:
        dt = datetime.fromisoformat(r["created_at"]).strftime("%d.%m %H:%M")
        note = f" ({r['note']})" if r["note"] else ""
        line = f"#{r['id']} • {dt} — {r['category']}: {fmt_money(r['amount'])}{note}"
        lines.append(line)
        # add delete button for this expense
        buttons.add(types.InlineKeyboardButton(f"❌ Удалить #{r['id']}", callback_data=f"del:{r['id']}"))

    msg_text = "🕒 Последние расходы:\n\n" + "\n".join(lines)
    bot.send_message(user_id, msg_text, reply_markup=buttons)


@bot.callback_query_handler(func=lambda call: call.data and (call.data.startswith("del:") or call.data.startswith("confirm_del:") or call.data == "show_history"))
def callback_delete_handler(call: types.CallbackQuery):
    data = call.data
    user_id = call.from_user.id

    if data == "show_history":
        # user pressed "Показать последние траты" from summary
        send_history_with_delete_buttons(user_id)
        bot.answer_callback_query(call.id)
        return

    if data.startswith("del:"):
        # Show confirmation inline
        _, exp_id_str = data.split(":", 1)
        try:
            exp_id = int(exp_id_str)
        except ValueError:
            bot.answer_callback_query(call.id, "Неверный id")
            return
        # Build confirmation keyboard
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("✅ Подтвердить удаление", callback_data=f"confirm_del:{exp_id}"))
        kb.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
        bot.send_message(user_id, f"Вы уверены, что хотите удалить запись #{exp_id}? Это действие необратимо.", reply_markup=kb)
        bot.answer_callback_query(call.id)
        return

    if data.startswith("confirm_del:"):
        _, exp_id_str = data.split(":", 1)
        try:
            exp_id = int(exp_id_str)
        except ValueError:
            bot.answer_callback_query(call.id, "Неверный id")
            return
        # Delete and notify
        delete_expense_by_id(exp_id)
        bot.send_message(user_id, f"✅ Запись #{exp_id} удалена.")
        bot.answer_callback_query(call.id)
        return

    if data == "cancel":
        bot.send_message(user_id, "Операция отменена.")
        bot.answer_callback_query(call.id)
        return

    bot.answer_callback_query(call.id, "Неизвестная команда.")


# --------------- HELP (separated fn) ---------------
def send_help(message: types.Message):
    bot.send_message(
        message.chat.id,
        "ℹ️ Поддерживаемые действия:\n"
        "- Быстрая запись: отправь '250 еда' → сохраню трату.\n"
        "- Кнопка '💰 Добавить трату' — пошаговый ввод (с подтверждением).\n"
        "- '📊 Моя статистика' — сводка трат по категориям; там можно открыть историю.\n"
        "- В истории у каждой траты есть кнопка '❌ Удалить' (подтверждение обязательно).\n"
        "- '💳 Установить бюджет' — задаёт месячный лимит. При превышении — уведомлю.\n",
        reply_markup=main_menu
    )


# --------------- START POLLING ---------------
if __name__ == "__main__":
    logger.info("Financial Guide запущен")
    try:
        bot.remove_webhook()
    except Exception:
        pass
    bot.infinity_polling(timeout=20, long_polling_timeout=20)
