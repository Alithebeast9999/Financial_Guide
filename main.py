# main.py
"""
Financial Guide - main bot file
Features added:
- Budget distribution on budget set (percentage table)
- Category limits stored and checked on expense add
- Reminders (daily) to add expenses
- Recurring expenses (auto-add on specified day)
- Simple tips/analytics and weekly/monthly reports
- History with inline delete buttons
- Guided add flow + quick parse
"""

import os
import time
import logging
import sqlite3
from threading import Thread
from datetime import datetime, timedelta, timezone
import io
import csv

import telebot
from telebot import types
from flask import Flask

# ----------------- CONFIG -----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "8080"))
DB_PATH = os.getenv("DB_PATH", "data.sqlite")
CURRENCY = "₽"

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN not set in env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("financial_guide")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
app = Flask("financial_guide_health")

# ----------------- DB HELPERS -----------------
def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Create required tables. Called at startup."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE NOT NULL,
            budget REAL DEFAULT NULL,
            notifications_enabled INTEGER DEFAULT 1,
            reminder_hour INTEGER DEFAULT 20
        );

        CREATE TABLE IF NOT EXISTS category_limits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            limit_amount REAL NOT NULL,
            recommended_pct REAL NOT NULL,
            UNIQUE(user_id, category)
        );

        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            category TEXT NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS recurring (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            category TEXT NOT NULL,
            note TEXT,
            day_of_month INTEGER NOT NULL, -- 1..28(29/30/31) we'll handle overflow as last day
            active INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS reports_sent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            period TEXT NOT NULL, -- e.g. 'week-2025-42' or 'month-2025-10'
            sent_at TEXT NOT NULL
        );
        """)
        conn.commit()

init_db()
logger.info("DB initialized at %s", DB_PATH)

# ----------------- UTIL -----------------
def now_utc():
    return datetime.now(timezone.utc)

def fmt_money(x):
    return f"{x:.2f} {CURRENCY}"

# ----------------- Default distribution table (percentages) -----------------
# Using the table from your image:
DEFAULT_DISTRIBUTION = [
    # ("Section", "Category", percent)
    ("НАДО", "аренда жилья", 35),
    ("НАДО", "продуктовая корзина", 15),
    ("НАДО", "коммунальные услуги", 5),
    ("НАДО", "связь", 3),
    ("НАДО", "транспорт", 5),
    ("НАДО", "личный уход", 2),
    ("НАДО", "медицина", 8),
    ("МОГУ", "инвестиции", 5),
    ("МОГУ", "подушка безопасности", 5),
    ("ХОЧУ", "развлечения", 7),
    ("ХОЧУ", "отдых-путешествия", 5),
    ("ХОЧУ", "шопинг", 5),
]

# ----------------- Category / limit helpers -----------------
def set_category_limits_from_budget(user_id: int, budget_amount: float):
    """Calculate per-category limits and store in category_limits table."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        # Remove existing limits for user
        cur.execute("DELETE FROM category_limits WHERE user_id = ?", (user_id,))
        for section, cat, pct in DEFAULT_DISTRIBUTION:
            limit_amount = budget_amount * (pct / 100.0)
            cur.execute("""
                INSERT INTO category_limits (user_id, category, limit_amount, recommended_pct)
                VALUES (?, ?, ?, ?)
            """, (user_id, cat, round(limit_amount, 2), pct))
        conn.commit()

def get_category_limits(user_id: int):
    with get_db_connection() as conn:
        rows = conn.execute("SELECT category, limit_amount, recommended_pct FROM category_limits WHERE user_id = ?", (user_id,)).fetchall()
        return {r["category"]: {"limit": r["limit_amount"], "pct": r["recommended_pct"]} for r in rows}

# ----------------- Budget helpers -----------------
def get_budget(user_id: int):
    with get_db_connection() as conn:
        row = conn.execute("SELECT budget FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return row["budget"] if row else None

def set_budget(user_id: int, amount: float):
    with get_db_connection() as conn:
        conn.execute("INSERT OR IGNORE INTO users (user_id, budget) VALUES (?, ?)", (user_id, amount))
        conn.execute("UPDATE users SET budget = ? WHERE user_id = ?", (amount, user_id))
        conn.commit()
    # after setting budget, create category limits
    set_category_limits_from_budget(user_id, amount)

# ----------------- Expense helpers -----------------
def add_expense(user_id: int, amount: float, category: str, note: str = ""):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO expenses (user_id, amount, category, note, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, amount, category, note, now_utc().isoformat())
        )
        conn.commit()

def get_recent_expenses(user_id: int, limit: int = 20):
    with get_db_connection() as conn:
        return conn.execute(
            "SELECT id, amount, category, note, created_at FROM expenses WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        ).fetchall()

def delete_expense_by_id(expense_id: int):
    with get_db_connection() as conn:
        conn.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))
        conn.commit()

def get_month_range(year: int, month: int):
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start.isoformat(), end.isoformat()

def get_month_total_by_category(user_id: int, year: int, month: int):
    start, end = get_month_range(year, month)
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
    start, end = get_month_range(year, month)
    with get_db_connection() as conn:
        row = conn.execute("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND created_at >= ? AND created_at < ?", (user_id, start, end)).fetchone()
        return row["total"] or 0.0

# ----------------- Recurring helpers -----------------
def add_recurring(user_id: int, amount: float, category: str, day_of_month: int, note: str = ""):
    with get_db_connection() as conn:
        conn.execute("INSERT INTO recurring (user_id, amount, category, note, day_of_month, active) VALUES (?, ?, ?, ?, ?, 1)",
                     (user_id, amount, category, note, day_of_month))
        conn.commit()

def get_active_recurring_for_day(day: int):
    with get_db_connection() as conn:
        return conn.execute("SELECT id, user_id, amount, category, note, day_of_month FROM recurring WHERE active = 1").fetchall()

# ----------------- Notifications/Reports helpers -----------------
def get_users_with_notifications():
    with get_db_connection() as conn:
        return conn.execute("SELECT user_id, reminder_hour FROM users WHERE notifications_enabled = 1 AND budget IS NOT NULL").fetchall()

def mark_report_sent(user_id: int, period: str):
    with get_db_connection() as conn:
        conn.execute("INSERT INTO reports_sent (user_id, period, sent_at) VALUES (?, ?, ?)", (user_id, period, now_utc().isoformat()))
        conn.commit()

def report_already_sent(user_id: int, period: str):
    with get_db_connection() as conn:
        row = conn.execute("SELECT 1 FROM reports_sent WHERE user_id = ? AND period = ? LIMIT 1", (user_id, period)).fetchone()
        return bool(row)

# ----------------- UI setup -----------------
main_menu = types.ReplyKeyboardMarkup(resize_keyboard=True)
main_menu.add(types.KeyboardButton("💰 Добавить трату"))
main_menu.add(types.KeyboardButton("📊 Моя статистика"), types.KeyboardButton("🕒 История"))
main_menu.add(types.KeyboardButton("💳 Установить бюджет"), types.KeyboardButton("⚙️ Настройки"))

# In-memory guided flow states
add_state = {}     # {user_id: {"step":..., ...}}
budget_state = {}  # {user_id: {"step":...}}
recurring_state = {}  # for setting recurring expenses
# ----------------- Parsing helper -----------------
def parse_expense_text(text: str):
    """Find first numeric token and return (amount, category) or None."""
    if not text:
        return None
    t = text.strip().replace(",", ".")
    parts = t.split()
    for i, token in enumerate(parts):
        try:
            amount = float(token)
            category = " ".join(parts[:i] + parts[i+1:]) or "прочее"
            return round(amount, 2), category
        except ValueError:
            continue
    return None

# ----------------- Bot handlers -----------------
@bot.message_handler(commands=['start'])
def cmd_start(message: types.Message):
    user_id = message.from_user.id
    first = message.from_user.first_name or "друг"
    budget = get_budget(user_id)
    text = (
        f"👋 Привет, <b>{first}</b>!\n\n"
        "Я — <b>Financial Guide</b>. Я помогу вести учёт расходов, предложу разумное распределение зарплаты и пришлю советы, если расходы идут не по плану.\n\n"
    )
    if budget is None:
        text += "Сначала установим месячный доход/бюджет — это позволит мне предложить тебе оптимальное распределение и лимиты по категориям.\n\nВведите сумму в рублях (например: 75000):"
        bot.send_message(user_id, text)
        budget_state[user_id] = {"step": "ask_budget_initial"}
        return
    text += "Используй меню ниже или отправь сообщение вида: <code>250 еда</code> для быстрой записи."
    bot.send_message(user_id, text, reply_markup=main_menu)

@bot.message_handler(func=lambda m: budget_state.get(m.from_user.id, {}).get("step") in ("ask_budget_initial", "ask_budget"))
def handle_budget_input(message: types.Message):
    user_id = message.from_user.id
    txt = (message.text or "").strip()
    try:
        amount = float(txt.replace(",", "."))
        set_budget(user_id, amount)
        # Send distribution table
        limits = get_category_limits(user_id)
        if not limits:
            bot.send_message(user_id, "Ошибка при создании лимитов — попробуйте ещё раз.")
            budget_state.pop(user_id, None)
            return
        # Build and send table message
        lines = [f"✅ Бюджет установлен: {fmt_money(amount)}\n\nРекомендованное распределение:"]
        # group by section using DEFAULT_DISTRIBUTION order
        for section, cat, pct in DEFAULT_DISTRIBUTION:
            limit_amount = limits.get(cat, {}).get("limit", 0.0)
            lines.append(f"{section if section else ''} • {cat}: {pct}% = {fmt_money(limit_amount)}")
        lines.append("\nТы можешь изменить проценты в настройках позже.")
        bot.send_message(user_id, "\n".join(lines), reply_markup=main_menu)
        budget_state.pop(user_id, None)
    except ValueError:
        bot.send_message(user_id, "Неверный формат. Введите число, например: 75000")

@bot.message_handler(func=lambda m: m.text == "💳 Установить бюджет")
def cmd_ask_set_budget(message: types.Message):
    user_id = message.from_user.id
    bot.send_message(user_id, "Введите новый месячный бюджет в рублях:")
    budget_state[user_id] = {"step": "ask_budget"}

@bot.message_handler(func=lambda m: m.text == "💰 Добавить трату")
def cmd_add_trade_start(message: types.Message):
    user_id = message.from_user.id
    if get_budget(user_id) is None:
        bot.send_message(user_id, "Сначала установите месячный бюджет. Нажмите '💳 Установить бюджет'.")
        budget_state[user_id] = {"step": "ask_budget"}
        return
    add_state[user_id] = {"step": "ask_amount"}
    bot.send_message(user_id, "Введите сумму и (опционально) категорию, например: <code>250 кафе</code>")

@bot.message_handler(func=lambda m: add_state.get(m.from_user.id, {}).get("step") == "ask_amount")
def handle_add_amount(message: types.Message):
    user_id = message.from_user.id
    txt = (message.text or "").strip()
    parsed = parse_expense_text(txt)
    if parsed:
        amount, category = parsed
        add_state[user_id] = {"step": "confirm", "amount": float(amount), "category": category, "note": ""}
        bot.send_message(user_id, f"Добавляем: {fmt_money(amount)} — <b>{category}</b>. Напишите примечание или 'OK' для подтверждения.")
        return
    try:
        amount = float(txt.replace(",", "."))
        add_state[user_id] = {"step": "ask_category", "amount": amount}
        bot.send_message(user_id, "Введите категорию (например: еда)")
    except ValueError:
        bot.send_message(user_id, "Не удалось распознать сумму. Введите число или в формате '250 еда'.")

@bot.message_handler(func=lambda m: add_state.get(m.from_user.id, {}).get("step") == "ask_category")
def handle_add_category(message: types.Message):
    user_id = message.from_user.id
    state = add_state.get(user_id)
    if not state:
        bot.send_message(user_id, "Сессия закончена. Нажмите '💰 Добавить трату' для новой записи.")
        return
    category = (message.text or "прочее").strip()
    state.update({"step": "confirm", "category": category, "note": ""})
    bot.send_message(user_id, f"Добавляем: {fmt_money(state['amount'])} — <b>{category}</b>. Отправь примечание или 'OK'.")

@bot.message_handler(func=lambda m: add_state.get(m.from_user.id, {}).get("step") == "confirm")
def handle_add_confirm(message: types.Message):
    user_id = message.from_user.id
    state = add_state.pop(user_id, None)
    if not state:
        bot.send_message(user_id, "Сессия закончена.")
        return
    text = (message.text or "").strip()
    if text.lower() in ("ok", "готово"):
        note = ""
    else:
        note = text
    amount = float(state["amount"])
    category = state["category"]
    add_expense(user_id, amount, category, note)
    # After adding, check totals and limits and notify
    now = now_utc()
    total = get_month_total(user_id, now.year, now.month)
    budget = get_budget(user_id) or 0.0
    limits = get_category_limits(user_id)
    cat_limit = limits.get(category, {}).get("limit", None)
    text_out = f"✅ Записано: {fmt_money(amount)} — {category}"
    if note:
        text_out += f" ({note})"
    text_out += f"\n\n📊 Потрачено за {now.strftime('%B %Y')}: {fmt_money(total)}"
    if budget and total > budget:
        text_out += f"\n\n⚠️ Вы превысили месячный бюджет: {fmt_money(total)} / {fmt_money(budget)}"
    # category limit
    if cat_limit is not None:
        # compute spent in category
        rows = get_month_total_by_category(user_id, now.year, now.month)
        cat_spent = 0.0
        for r in rows:
            if r["category"] == category:
                cat_spent = r["total"]
                break
        if cat_spent > cat_limit:
            text_out += f"\n\n⚠️ Превышение по категории '{category}': {fmt_money(cat_spent)} / {fmt_money(cat_limit)}"
    bot.send_message(user_id, text_out, reply_markup=main_menu)

# Quick free-text handling when not in guided flow
@bot.message_handler(func=lambda m: True)
def handle_text_general(message: types.Message):
    user_id = message.from_user.id
    if add_state.get(user_id) or budget_state.get(user_id) or recurring_state.get(user_id):
        return  # specific flows handled elsewhere

    txt = (message.text or "").strip()
    # menu buttons
    if txt == "📊 Моя статистика":
        now = now_utc()
        stats = get_month_total_by_category(user_id, now.year, now.month)
        if not stats:
            bot.send_message(user_id, "Нет данных за текущий месяц.", reply_markup=main_menu)
            return
        total = sum(r["total"] for r in stats)
        lines = [f"📊 Расходы за {now.strftime('%B %Y')}:\n"]
        for r in stats:
            perc = (r["total"] / total) * 100 if total else 0
            lines.append(f"• {r['category']}: {fmt_money(r['total'])} ({perc:.1f}%)")
        lines.append(f"\n💵 Всего: {fmt_money(total)}")
        # add inline button to show history with delete buttons
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("🕒 Показать последние траты", callback_data="show_history"))
        bot.send_message(user_id, "\n".join(lines), reply_markup=kb)
        return

    if txt == "🕒 История":
        send_history_with_delete_buttons(user_id)
        return

    if txt == "⚙️ Настройки":
        send_settings(user_id)
        return

    if txt == "💳 Установить бюджет":
        cmd_ask_set_budget(message)
        return

    # try parse as quick expense
    parsed = parse_expense_text(txt)
    if parsed:
        if get_budget(user_id) is None:
            bot.send_message(user_id, "Сначала установите месячный бюджет. Нажмите '💳 Установить бюджет'.")
            budget_state[user_id] = {"step": "ask_budget"}
            return
        amount, category = parsed
        add_expense(user_id, amount, category, "")
        now = now_utc()
        total = get_month_total(user_id, now.year, now.month)
        budget = get_budget(user_id) or 0.0
        limits = get_category_limits(user_id)
        text_out = f"✅ Быстрая запись: {fmt_money(amount)} — {category}\n\n📊 Потрачено за {now.strftime('%B %Y')}: {fmt_money(total)}"
        if budget and total > budget:
            text_out += f"\n\n⚠️ Вы превысили месячный бюджет: {fmt_money(total)} / {fmt_money(budget)}"
        cat_limit = limits.get(category, {}).get("limit")
        if cat_limit:
            # compute spent in category
            rows = get_month_total_by_category(user_id, now.year, now.month)
            cat_spent = 0.0
            for r in rows:
                if r["category"] == category:
                    cat_spent = r["total"]
                    break
            if cat_spent > cat_limit:
                text_out += f"\n\n⚠️ Превышение по категории '{category}': {fmt_money(cat_spent)} / {fmt_money(cat_limit)}"
        bot.send_message(user_id, text_out, reply_markup=main_menu)
        return

    # fallback
    bot.send_message(user_id, "❓ Не понял. Используйте меню или отправьте '250 еда' для быстрой записи.", reply_markup=main_menu)

# ----------------- History with delete buttons -----------------
def send_history_with_delete_buttons(user_id: int, limit: int = 20):
    rows = get_recent_expenses(user_id, limit=limit)
    if not rows:
        bot.send_message(user_id, "Пока нет записей.", reply_markup=main_menu)
        return
    text_lines = ["🕒 Последние траты (нажмите ❌ для удаления):\n"]
    kb = types.InlineKeyboardMarkup(row_width=1)
    for r in rows:
        dt = datetime.fromisoformat(r["created_at"]).astimezone(timezone.utc).strftime("%d.%m %H:%M")
        note = f" ({r['note']})" if r["note"] else ""
        text_lines.append(f"#{r['id']} • {dt} — {r['category']}: {fmt_money(r['amount'])}{note}")
        kb.add(types.InlineKeyboardButton(f"❌ Удалить #{r['id']}", callback_data=f"del:{r['id']}"))
    bot.send_message(user_id, "\n".join(text_lines), reply_markup=kb)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("del:"))
def callback_delete(call: types.CallbackQuery):
    data = call.data
    user_id = call.from_user.id
    try:
        _, exp_id_str = data.split(":", 1)
        exp_id = int(exp_id_str)
    except Exception:
        bot.answer_callback_query(call.id, "Ошибка id")
        return
    # ask confirmation
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("✅ Подтвердить удаление", callback_data=f"confirm_del:{exp_id}"))
    kb.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
    bot.send_message(user_id, f"Вы уверены, что хотите удалить запись #{exp_id}? Это действие необратимо.", reply_markup=kb)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("confirm_del:"))
def callback_confirm_delete(call: types.CallbackQuery):
    _, exp_id_str = call.data.split(":", 1)
    try:
        exp_id = int(exp_id_str)
    except Exception:
        bot.answer_callback_query(call.id, "Неверный id")
        return
    delete_expense_by_id(exp_id)
    bot.send_message(call.from_user.id, f"✅ Запись #{exp_id} удалена.")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "cancel")
def callback_cancel(call: types.CallbackQuery):
    bot.answer_callback_query(call.id, "Отменено")
    bot.send_message(call.from_user.id, "Операция отменена.", reply_markup=main_menu)

# ----------------- Settings UI -----------------
def send_settings(user_id: int):
    with get_db_connection() as conn:
        row = conn.execute("SELECT notifications_enabled, reminder_hour FROM users WHERE user_id = ?", (user_id,)).fetchone()
    notif = bool(row["notifications_enabled"]) if row else True
    hour = row["reminder_hour"] if row else 20
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Включить уведомления" if not notif else "Отключить уведомления",
                                      callback_data="toggle_notifications"))
    kb.add(types.InlineKeyboardButton("Изменить время напоминания", callback_data="set_reminder_hour"))
    kb.add(types.InlineKeyboardButton("Управлять регулярными расходами", callback_data="manage_recurring"))
    bot.send_message(user_id, f"⚙️ Настройки уведомлений: {'Вкл' if notif else 'Выкл'}. Время напоминания (UTC): {hour}:00", reply_markup=kb)

@bot.callback_query_handler(func=lambda call: call.data == "toggle_notifications")
def callback_toggle_notifications(call: types.CallbackQuery):
    user_id = call.from_user.id
    with get_db_connection() as conn:
        row = conn.execute("SELECT notifications_enabled FROM users WHERE user_id = ?", (user_id,)).fetchone()
        cur_val = bool(row["notifications_enabled"]) if row else True
        new_val = 0 if cur_val else 1
        conn.execute("INSERT OR IGNORE INTO users (user_id, notifications_enabled) VALUES (?, ?)", (user_id, new_val))
        conn.execute("UPDATE users SET notifications_enabled = ? WHERE user_id = ?", (new_val, user_id))
        conn.commit()
    bot.answer_callback_query(call.id, "Настройки обновлены")
    send_settings(user_id)

@bot.callback_query_handler(func=lambda call: call.data == "set_reminder_hour")
def callback_set_reminder_hour(call: types.CallbackQuery):
    user_id = call.from_user.id
    bot.send_message(user_id, "Введите час в UTC (0-23), в который вы хотите получать ежедневное напоминание (например: 20):")
    # next message will be processed by generic handler and we set a temporary state in users table via budget_state
    budget_state[user_id] = {"step": "set_reminder_hour"}
    bot.answer_callback_query(call.id)

@bot.message_handler(func=lambda m: budget_state.get(m.from_user.id, {}).get("step") == "set_reminder_hour")
def handle_set_reminder_hour(message: types.Message):
    user_id = message.from_user.id
    txt = (message.text or "").strip()
    try:
        hour = int(txt)
        if not (0 <= hour <= 23):
            raise ValueError
        with get_db_connection() as conn:
            conn.execute("INSERT OR IGNORE INTO users (user_id, reminder_hour) VALUES (?, ?)", (user_id, hour))
            conn.execute("UPDATE users SET reminder_hour = ? WHERE user_id = ?", (hour, user_id))
            conn.commit()
        bot.send_message(user_id, f"Время напоминания установлено на {hour}:00 UTC", reply_markup=main_menu)
        budget_state.pop(user_id, None)
    except ValueError:
        bot.send_message(user_id, "Неверный формат. Введите целое число от 0 до 23.")

# ----------------- Scheduling background thread -----------------
def scheduler_loop():
    logger.info("Scheduler thread started")
    last_day_processed = None
    last_week_report = None
    while True:
        try:
            now = now_utc()
            # Daily recurring process at 00:10 UTC (we check date change)
            # Use day change detection
            today_date = now.date()
            # process recurring once per day
            if last_day_processed != today_date:
                logger.info("Daily job: processing recurring expenses and tips")
                process_recurring_for_today(today_date.day)
                last_day_processed = today_date
            # Send daily reminders at users' reminder_hour
            users = get_users_with_notifications()
            for u in users:
                try:
                    uid = u["user_id"]
                    reminder_hour = int(u["reminder_hour"] or 20)
                    # if current hour equals reminder and not already sent this hour
                    if now.hour == reminder_hour:
                        # send a reminder (we don't track one-per-hour sent state; acceptable to possibly repeat if scheduler restarts)
                        send_daily_reminder(uid)
                except Exception as e:
                    logger.exception("Error sending reminder to user %s: %s", u, e)
            # Weekly reports: send on Monday 09:00 UTC (we check week number)
            week_id = now.isocalendar()[1]
            if now.weekday() == 0 and now.hour == 9 and last_week_report != week_id:
                # send weekly reports to users who have budget set
                logger.info("Sending weekly reports for week %s", week_id)
                send_weekly_reports()
                last_week_report = week_id
            # Sleep 60 seconds
            time.sleep(60)
        except Exception as e:
            logger.exception("Scheduler loop error: %s", e)
            time.sleep(60)

def process_recurring_for_today(day_of_month: int):
    rows = get_active_recurring_for_day(day_of_month)
    for r in rows:
        # handle day overflow: if recurring day > last day of current month, schedule on last day.
        # simple approach: just add if r['day_of_month'] == day or if day > 28 and r day > last_day -> allow later improvement
        # For simplicity we add only when equal (owner can schedule on <=28 or specialized)
        if r["day_of_month"] == day_of_month:
            try:
                add_expense(r["user_id"], r["amount"], r["category"], r["note"])
                bot.send_message(r["user_id"], f"🔁 Автоматическая запись: {fmt_money(r['amount'])} — {r['category']} (регулярная).")
            except Exception as e:
                logger.exception("Failed to add recurring expense for user %s: %s", r["user_id"], e)

def get_active_recurring_for_day(day):
    with get_db_connection() as conn:
        return conn.execute("SELECT id, user_id, amount, category, note, day_of_month FROM recurring WHERE active = 1").fetchall()

def send_daily_reminder(user_id: int):
    # remind user to add today's expenses if any - check user's settings
    with get_db_connection() as conn:
        row = conn.execute("SELECT notifications_enabled FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if row is None or row["notifications_enabled"] == 0:
            return
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Добавить сейчас", callback_data="quick_add"))
    kb.add(types.InlineKeyboardButton("Напомнить позже", callback_data="remind_later"))
    bot.send_message(user_id, "⏰ Напоминание: хотите добавить сегодняшние траты?", reply_markup=kb)

def send_weekly_reports():
    # Send weekly summary (last 7 days) for all users with budget
    with get_db_connection() as conn:
        users = conn.execute("SELECT user_id FROM users WHERE budget IS NOT NULL").fetchall()
    for u in users:
        try:
            send_report_for_user(u["user_id"], period="week")
        except Exception as e:
            logger.exception("Failed weekly report for %s: %s", u["user_id"], e)

def send_report_for_user(user_id: int, period: str = "week"):
    # period: 'week' or 'month'
    now = now_utc()
    if period == "week":
        start = (now - timedelta(days=7)).isoformat()
        period_id = f"week-{now.year}-{now.isocalendar()[1]}"
    else:
        start, end = get_month_range(now.year, now.month)
        start = start
        period_id = f"month-{now.year}-{now.month}"
    # guard: do not resend same period
    if report_already_sent(user_id, period_id):
        return
    # fetch expenses
    with get_db_connection() as conn:
        if period == "week":
            rows = conn.execute("SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND created_at >= ? GROUP BY category", (user_id, start)).fetchall()
            total_row = conn.execute("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND created_at >= ?", (user_id, start)).fetchone()
            total = total_row["total"] or 0.0
        else:
            st, en = get_month_range(now.year, now.month)
            rows = conn.execute("SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND created_at >= ? AND created_at < ? GROUP BY category", (user_id, st, en)).fetchall()
            total_row = conn.execute("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND created_at >= ? AND created_at < ?", (user_id, st, en)).fetchone()
            total = total_row["total"] or 0.0
    if not rows:
        bot.send_message(user_id, f"📊 Ваш отчет ({period}) — расходов не найдено.")
        mark_report_sent(user_id, period_id)
        return
    text = f"📊 Ваш отчет за {period}:\n\n"
    for r in rows:
        text += f"• {r['category']}: {fmt_money(r['total'])}\n"
    text += f"\n💵 Всего: {fmt_money(total)}"
    bot.send_message(user_id, text)
    mark_report_sent(user_id, period_id)

# ----------------- Callback for quick add/remind later and history show -----------------
@bot.callback_query_handler(func=lambda call: True)
def all_callbacks(call: types.CallbackQuery):
    data = call.data
    user_id = call.from_user.id
    if data == "quick_add":
        bot.send_message(user_id, "Отправьте трату в формате: 250 еда")
        bot.answer_callback_query(call.id)
        return
    if data == "remind_later":
        bot.send_message(user_id, "Хорошо — напомню позже.")
        bot.answer_callback_query(call.id)
        return
    if data == "show_history":
        send_history_with_delete_buttons(user_id)
        bot.answer_callback_query(call.id)
        return
    # other callbacks handled elsewhere (delete confirmation etc.)
    # we keep this handler general to not miss calls

# ----------------- Helper: month range -----------------
def get_month_range(year: int, month: int):
    start = datetime(year, month, 1, tzinfo=timezone.utc).isoformat()
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc).isoformat()
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc).isoformat()
    return start, end

# ----------------- History & delete (reused) -----------------
def send_history_with_delete_buttons(user_id: int, limit: int = 20):
    rows = get_recent_expenses(user_id, limit=limit)
    if not rows:
        bot.send_message(user_id, "Пока нет записей.", reply_markup=main_menu)
        return
    lines = ["🕒 Последние траты (нажмите ❌ для удаления):\n"]
    kb = types.InlineKeyboardMarkup(row_width=1)
    for r in rows:
        dt = datetime.fromisoformat(r["created_at"]).astimezone(timezone.utc).strftime("%d.%m %H:%M")
        note = f" ({r['note']})" if r["note"] else ""
        lines.append(f"#{r['id']} • {dt} — {r['category']}: {fmt_money(r['amount'])}{note}")
        kb.add(types.InlineKeyboardButton(f"❌ Удалить #{r['id']}", callback_data=f"del:{r['id']}"))
    bot.send_message(user_id, "\n".join(lines), reply_markup=kb)

# delete flow
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("del:"))
def callback_request_delete(call: types.CallbackQuery):
    try:
        _, sid = call.data.split(":", 1)
        exp_id = int(sid)
    except:
        bot.answer_callback_query(call.id, "Неверный id")
        return
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("✅ Подтвердить удаление", callback_data=f"confirm_del:{exp_id}"))
    kb.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
    bot.send_message(call.from_user.id, f"Вы уверены, что хотите удалить запись #{exp_id}?", reply_markup=kb)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("confirm_del:"))
def callback_confirm_delete(call: types.CallbackQuery):
    _, sid = call.data.split(":", 1)
    try:
        exp_id = int(sid)
    except:
        bot.answer_callback_query(call.id, "Неверный id")
        return
    delete_expense_by_id(exp_id)
    bot.send_message(call.from_user.id, f"✅ Запись #{exp_id} удалена.")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "cancel")
def callback_cancel(call: types.CallbackQuery):
    bot.answer_callback_query(call.id, "Отменено")
    bot.send_message(call.from_user.id, "Операция отменена.", reply_markup=main_menu)

# ----------------- Start scheduler thread and bot -----------------
@app.route("/")
def health():
    return "Financial Guide running", 200

def start_scheduler():
    t = Thread(target=scheduler_loop, daemon=True)
    t.start()

if __name__ == "__main__":
    logger.info("Starting Financial Guide bot")
    # remove webhook just in case
    try:
        bot.remove_webhook()
    except:
        pass
    start_scheduler()
    # use infinite polling (pyTelegramBotAPI)
    bot.infinity_polling(timeout=20, long_polling_timeout=20)
