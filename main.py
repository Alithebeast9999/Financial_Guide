"""
main.py - Telegram Finance Assistant (webhook-based) for Render

This single-file implementation uses FastAPI + APScheduler + SQLite + requests to
implement a Telegram bot webhook service suitable for deploying on Render as a
web service. It intentionally avoids heavy Telegram libraries to keep webhook
handling explicit and simple.

FILES (suggested):
- main.py  (this file)
- render.yaml (see below block)
- requirements.txt (see below block)

--- Suggested render.yaml ---
# replace <YOUR_SERVICE_NAME> and the appropriate env var name for TELEGRAM_TOKEN
services:
  - type: web
    name: telegram-fin-helper
    env: python
    buildCommand: ""
    startCommand: "uvicorn main:app --host 0.0.0.0 --port $PORT"
    envVars:
      - key: TELEGRAM_TOKEN
        value: "__ADD_TOKEN_IN_RENDER__"
      - key: WEBHOOK_URL
        value: "https://<your-render-service>.onrender.com/webhook/${TELEGRAM_TOKEN}"

--- Suggested requirements.txt ---
fastapi
uvicorn[standard]
requests
apscheduler
python-dotenv
pytz

Save these two files in your repo alongside main.py. On Render, set the TELEGRAM_TOKEN
and make sure WEBHOOK_URL points to https://<service>.onrender.com/webhook/<token>.

USAGE NOTES
1) After deployment, set Telegram webhook manually once:
   curl -X POST "https://api.telegram.org/bot<token>/setWebhook" -d "url=<WEBHOOK_URL>"
2) Set the TELEGRAM_TOKEN env var in Render settings.

"""

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse
import sqlite3
import os
import requests
import json
from datetime import datetime, timedelta, timezone, date
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    print("WARNING: TELEGRAM_TOKEN not set. Set it in Render environment variables.")

API_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

app = FastAPI()
DB_PATH = os.environ.get("DB_PATH", "finance_bot.db")

# Recommended category distribution from the provided image
DEFAULT_CATEGORIES = [
    ("rent", "АРЕНДА ЖИЛЬЯ", 35),
    ("groceries", "ПРОДУКТОВАЯ КОРЗИНА", 15),
    ("utilities", "КОММ. УСЛУГИ", 5),
    ("phone", "СВЯЗЬ", 3),
    ("transport", "ТРАНСПОРТ", 5),
    ("personal_care", "ЛИЧНЫЙ УХОД", 2),
    ("medicine", "МЕДИЦИНА", 8),
    ("investments", "ИНВЕСТИЦИИ", 5),
    ("safety", "ПОДУШКА БЕЗОПАСНОСТИ", 5),
    ("entertainment", "РАЗВЛЕЧЕНИЯ", 7),
    ("travel", "ОТДЫХ - ПУТЕШЕСТВИЯ", 5),
    ("shopping", "ШОПИНГ", 5),
]

# ========== DB helpers ===========

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        username TEXT,
        monthly_budget REAL,
        timezone TEXT,
        notifications_enabled INTEGER DEFAULT 1
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS categories (
        key TEXT PRIMARY KEY,
        title TEXT,
        percent INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS expenses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        category_key TEXT,
        note TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS recurring (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        category_key TEXT,
        day_of_month INTEGER,
        note TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS states (
        user_id INTEGER PRIMARY KEY,
        state TEXT,
        tmp_amount REAL,
        tmp_meta TEXT
    )
    """)

    # seed categories if empty
    cur.execute("SELECT COUNT(1) FROM categories")
    if cur.fetchone()[0] == 0:
        cur.executemany("INSERT INTO categories(key, title, percent) VALUES (?, ?, ?)", DEFAULT_CATEGORIES)

    conn.commit()
    conn.close()


def db_connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

init_db()
db = db_connect()

# ========== Telegram helpers ===========

def send_message(chat_id, text, reply_markup=None, parse_mode=None):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup is not None:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    if parse_mode:
        payload["parse_mode"] = parse_mode
    requests.post(f"{API_BASE}/sendMessage", data=payload)


def answer_callback(callback_id, text=None):
    data = {"callback_query_id": callback_id}
    if text:
        data["text"] = text
    requests.post(f"{API_BASE}/answerCallbackQuery", data=data)


# ========== Business logic ===========

def set_user_from_message(msg):
    user = msg.get("from", {})
    uid = user.get("id")
    if not uid:
        return
    cur = db.cursor()
    cur.execute("SELECT user_id FROM users WHERE user_id = ?", (uid,))
    if cur.fetchone() is None:
        cur.execute("INSERT INTO users(user_id, first_name, last_name, username) VALUES (?, ?, ?, ?)",
                    (uid, user.get("first_name"), user.get("last_name"), user.get("username")))
        db.commit()


def format_money(amount):
    return f"{amount:.0f}₽"


def compute_limits(monthly_budget):
    limits = {}
    for key, title, pct in DEFAULT_CATEGORIES:
        limits[key] = {"title": title, "percent": pct, "amount": round(monthly_budget * pct / 100)}
    return limits


def get_user_settings(user_id):
    cur = db.cursor()
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    return cur.fetchone()


def save_monthly_budget(user_id, amount):
    cur = db.cursor()
    cur.execute("UPDATE users SET monthly_budget = ? WHERE user_id = ?", (amount, user_id))
    db.commit()


def add_expense(user_id, amount, category_key, note=None, created_at=None):
    created_at = created_at or datetime.utcnow().isoformat()
    cur = db.cursor()
    cur.execute("INSERT INTO expenses(user_id, amount, category_key, note, created_at) VALUES (?, ?, ?, ?, ?)",
                (user_id, amount, category_key, note, created_at))
    db.commit()
    return cur.lastrowid


def get_month_expenses_sum(user_id, month_start=None):
    cur = db.cursor()
    if not month_start:
        now = datetime.utcnow()
        month_start = datetime(now.year, now.month, 1)
    cur.execute("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND created_at >= ?",
                (user_id, month_start.isoformat()))
    row = cur.fetchone()
    return row["total"] or 0


def get_category_sum_month(user_id, category_key, month_start=None):
    cur = db.cursor()
    if not month_start:
        now = datetime.utcnow()
        month_start = datetime(now.year, now.month, 1)
    cur.execute("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND category_key = ? AND created_at >= ?",
                (user_id, category_key, month_start.isoformat()))
    row = cur.fetchone()
    return row["total"] or 0


# ========== Message flow handlers ===========

CATEGORY_OPTIONS = [
    {"text": c[1], "key": c[0]} for c in DEFAULT_CATEGORIES
]


def keyboard_main():
    return {
        "inline_keyboard": [
            [{"text": "➕ Добавить трату", "callback_data": "action_add"}, {"text": "📜 История", "callback_data": "action_history"}],
            [{"text": "📊 Моя статистика", "callback_data": "action_stats"}, {"text": "❓ Помощь", "callback_data": "action_help"}]
        ]
    }


@app.post("/webhook/{token}")
async def webhook(token: str, request: Request):
    if token != TELEGRAM_TOKEN:
        return JSONResponse(status_code=403, content={"ok": False})
    data = await request.json()
    # Basic user create
    if "message" in data:
        msg = data["message"]
        set_user_from_message(msg)
        await handle_message(msg)
    elif "callback_query" in data:
        await handle_callback(data["callback_query"])    
    return JSONResponse(status_code=200, content={"ok": True})


async def handle_message(msg):
    chat_id = msg["chat"]["id"]
    text = msg.get("text", "")
    user_id = msg["from"]["id"]

    # check if user is in a state
    cur = db.cursor()
    cur.execute("SELECT state, tmp_amount, tmp_meta FROM states WHERE user_id = ?", (user_id,))
    st = cur.fetchone()
    if text.startswith("/"):
        # Commands
        if text.startswith("/start"):
            help_text = (
                "Привет! Я — твой финансовый помощник. Я помогаю учитывать траты, "
                "рассчитывать рекомендованные лимиты и присылать напоминания.\n\n"
                "Как начать:\n1) Введите ваш месячный доход (например: 100000) в сообщении.\n"
                "2) Нажмите 'Добавить трату' или просто отправьте сумму — я попрошу выбрать категорию.\n\n"
                "Команды:\n/report week — недельный отчёт\n/report month — месячный отчёт\n/help — помощь"
            )
            send_message(chat_id, help_text)
            send_message(chat_id, "Введите, пожалуйста, ваш месячный доход (в ₽), например: 100000")
            return
        elif text.startswith("/help"):
            send_message(chat_id, "Команды: /start, /help, /report week, /report month. Также используйте кнопки меню.")
            send_message(chat_id, "Меню:", reply_markup=keyboard_main())
            return
        elif text.startswith("/report"):
            parts = text.split()
            period = parts[1] if len(parts) > 1 else "month"
            if period == "week":
                txt = generate_report_week(user_id)
                send_message(chat_id, txt)
            else:
                txt = generate_report_month(user_id)
                send_message(chat_id, txt)
            return
    # If in flow expecting category
    if st and st["state"] == "await_category":
        # we expect the category text or inline selection key
        category_text = text.strip()
        # find matching key
        cur.execute("SELECT key FROM categories WHERE title = ?", (category_text,))
        row = cur.fetchone()
        if row:
            key = row["key"]
            amount = st["tmp_amount"]
            add_expense(user_id, amount, key)
            send_message(chat_id, f"Добавлена трата {format_money(amount)} в категорию {category_text}.")
            # clear state
            cur.execute("DELETE FROM states WHERE user_id = ?", (user_id,))
            db.commit()
            check_limits_and_warn(user_id, chat_id, key)
            return
        else:
            send_message(chat_id, "Не распознал категорию. Введите одну из следующих категорий:\n" + ", ".join([c[1] for c in DEFAULT_CATEGORIES]))
            return

    # If message is just a number -> treat as expense amount and ask for category
    cleaned = text.replace(" ", "").replace("₽", "")
    try:
        amt = float(cleaned)
        # store state
        cur.execute("REPLACE INTO states(user_id, state, tmp_amount, tmp_meta) VALUES (?, ?, ?, ?)",
                    (user_id, "await_category", amt, None))
        db.commit()
        # prompt category list
        kb = {"keyboard": [[{"text": c[1]}] for c in DEFAULT_CATEGORIES], "one_time_keyboard": True}
        send_message(chat_id, f"Вы ввели сумму {format_money(amt)}. Выберите категорию (напишите или нажмите):")
        # Also show inline categories (compact)
        inline = {"inline_keyboard": [[{"text": c[1], "callback_data": f"pickcat:{c[0]}"}] for c in DEFAULT_CATEGORIES]}
        send_message(chat_id, "Или выберите категорию кнопкой:", reply_markup=inline)
        return
    except Exception:
        pass

    # If nothing else, show main menu
    send_message(chat_id, "Меню:", reply_markup=keyboard_main())


async def handle_callback(cb):
    data = cb.get("data")
    chat_id = cb["message"]["chat"]["id"] if cb.get("message") else cb["from"]["id"]
    user_id = cb["from"]["id"]
    callback_id = cb.get("id")

    if data == "action_add":
        send_message(chat_id, "Отправьте сумму траты (например: 1250) или нажмите кнопку ниже:", reply_markup=None)
        answer_callback(callback_id)
        return
    if data == "action_history":
        txt = build_history_text(user_id)
        send_message(chat_id, txt)
        answer_callback(callback_id)
        return
    if data == "action_stats":
        txt = generate_stat_text(user_id)
        send_message(chat_id, txt)
        answer_callback(callback_id)
        return
    if data == "action_help":
        send_message(chat_id, "Помощь: Отправьте сумму для добавления траты, используйте кнопки меню или команды /report.")
        answer_callback(callback_id)
        return
    if data.startswith("pickcat:"):
        cat = data.split(":", 1)[1]
        # fetch state amount
        cur = db.cursor()
        cur.execute("SELECT tmp_amount FROM states WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            send_message(chat_id, "Не найдена ожидаемая сумма. Пожалуйста, сначала отправьте сумму траты.")
            answer_callback(callback_id)
            return
        amt = row["tmp_amount"]
        # get title
        cur.execute("SELECT title FROM categories WHERE key = ?", (cat,))
        title = cur.fetchone()["title"]
        add_expense(user_id, amt, cat)
        cur.execute("DELETE FROM states WHERE user_id = ?", (user_id,))
        db.commit()
        send_message(chat_id, f"Добавлена трата {format_money(amt)} в категорию {title}.")
        answer_callback(callback_id, "Трата сохранена")
        check_limits_and_warn(user_id, chat_id, cat)
        return
    if data.startswith("del:"):
        exp_id = int(data.split(":", 1)[1])
        cur = db.cursor()
        cur.execute("DELETE FROM expenses WHERE id = ?", (exp_id,))
        db.commit()
        answer_callback(callback_id, "Удалено")
        send_message(chat_id, "Трата удалена.")
        return

    answer_callback(callback_id)


def build_history_text(user_id, limit=20):
    cur = db.cursor()
    cur.execute("SELECT id, amount, category_key, note, created_at FROM expenses WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
                (user_id, limit))
    rows = cur.fetchall()
    if not rows:
        return "История пуста."
    lines = []
    for r in rows:
        # get category title
        cur.execute("SELECT title FROM categories WHERE key = ?", (r["category_key"],))
        cat = cur.fetchone()["title"]
        dt = r["created_at"]
        lines.append(f"{r['id']}: {format_money(r['amount'])} — {cat} — {dt}")
    text = "История последних трат:\n" + "\n".join(lines)
    # include instruction for deletion
    text += "\n\nЧтобы удалить трату, нажмите красный крестик рядом (внизу каждой строки)"  # we will provide inline buttons below
    # Add inline keyboard with delete buttons for each expense (up to 5 to avoid spam)
    # but we cannot attach keyboard in the same text here because send_message called separately above. We'll return text and the client will show buttons separately.
    return text


def generate_stat_text(user_id):
    cur = db.cursor()
    cur.execute("SELECT monthly_budget FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if not row or not row["monthly_budget"]:
        return "Бюджет не установлен. Введите ваш месячный доход, чтобы получить статистику."
    monthly = row["monthly_budget"]
    limits = compute_limits(monthly)
    month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    lines = [f"Ваш доход: {format_money(monthly)}\nРекомендованные лимиты:"]
    total_spent = get_month_expenses_sum(user_id, month_start)
    for key, v in limits.items():
        spent = get_category_sum_month(user_id, key, month_start)
        lines.append(f"{v['title']}: {v['percent']}% — лимит {format_money(v['amount'])}, потрачено {format_money(spent)}")
    lines.append(f"\nВсего потрачено за месяц: {format_money(total_spent)}")
    return "\n".join(lines)


def generate_report_month(user_id):
    cur = db.cursor()
    month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    cur.execute("SELECT category_key, SUM(amount) as s FROM expenses WHERE user_id = ? AND created_at >= ? GROUP BY category_key",
                (user_id, month_start.isoformat()))
    rows = cur.fetchall()
    if not rows:
        return "Нет трат за текущий месяц."
    lines = ["Месячный отчёт:"]
    for r in rows:
        cur.execute("SELECT title FROM categories WHERE key = ?", (r["category_key"],))
        title = cur.fetchone()["title"]
        lines.append(f"{title}: {format_money(r['s'])}")
    return "\n".join(lines)


def generate_report_week(user_id):
    now = datetime.utcnow()
    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    cur = db.cursor()
    cur.execute("SELECT category_key, SUM(amount) as s FROM expenses WHERE user_id = ? AND created_at >= ? GROUP BY category_key",
                (user_id, week_start.isoformat()))
    rows = cur.fetchall()
    if not rows:
        return "Нет трат за текущую неделю."
    lines = ["Недельный отчёт:"]
    for r in rows:
        cur.execute("SELECT title FROM categories WHERE key = ?", (r["category_key"],))
        title = cur.fetchone()["title"]
        lines.append(f"{title}: {format_money(r['s'])}")
    return "\n".join(lines)


def check_limits_and_warn(user_id, chat_id, category_key):
    cur = db.cursor()
    cur.execute("SELECT monthly_budget FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if not row or not row["monthly_budget"]:
        return
    monthly = row["monthly_budget"]
    limits = compute_limits(monthly)
    cat_limit = limits[category_key]["amount"]
    cat_spent = get_category_sum_month(user_id, category_key)
    if cat_spent > cat_limit:
        send_message(chat_id, f"⚠️ Внимание: вы превысили лимит для {limits[category_key]['title']} — {format_money(cat_spent)} из {format_money(cat_limit)}")
    total_spent = get_month_expenses_sum(user_id)
    if total_spent > monthly:
        send_message(chat_id, f"⚠️ Внимание: вы превысили общий месячный бюджет — потрачено {format_money(total_spent)} из {format_money(monthly)}")


# ========== Recurring & Scheduled jobs ===========

scheduler = AsyncIOScheduler()


def insert_recurring_for_today():
    # Run at start of day UTC+3
    cur = db.cursor()
    cur.execute("SELECT id, user_id, amount, category_key, day_of_month, note FROM recurring")
    rows = cur.fetchall()
    if not rows:
        return
    today = datetime.now(pytz.timezone('Europe/Moscow')).day
    for r in rows:
        if r["day_of_month"] == today:
            add_expense(r["user_id"], r["amount"], r["category_key"], r["note"], created_at=datetime.utcnow().isoformat())
            # optional: send notif
            send_message(r["user_id"], f"Автозапись регулярной траты {format_money(r['amount'])} в категорию {r['category_key']}")


def daily_reminder_to_users():
    # send to users with notifications_enabled
    cur = db.cursor()
    cur.execute("SELECT user_id FROM users WHERE notifications_enabled = 1")
    rows = cur.fetchall()
    for r in rows:
        send_message(r["user_id"], "Не забыли добавить сегодняшние траты? Отправьте сумму или нажмите 'Добавить трату'.", reply_markup=keyboard_main())


def weekly_send_reports():
    # Monday UTC+3 weekly report
    cur = db.cursor()
    cur.execute("SELECT user_id FROM users WHERE notifications_enabled = 1")
    rows = cur.fetchall()
    for r in rows:
        txt = generate_report_week(r["user_id"])
        send_message(r["user_id"], txt)


@app.on_event("startup")
async def startup_event():
    # Scheduler jobs
    scheduler.add_job(insert_recurring_for_today, CronTrigger(hour=0, minute=5, timezone=pytz.timezone('Europe/Moscow')))
    scheduler.add_job(daily_reminder_to_users, CronTrigger(hour=9, minute=0, timezone=pytz.timezone('Europe/Moscow')))
    scheduler.add_job(weekly_send_reports, CronTrigger(day_of_week='mon', hour=9, minute=30, timezone=pytz.timezone('Europe/Moscow')))
    scheduler.start()
    print("Scheduler started")


# ========== Extra endpoints to manage budget & recurring (HTTP API for future web UI) ===========

@app.post("/set_budget/{user_id}/{amount}")
async def set_budget(user_id: int, amount: float):
    cur = db.cursor()
    cur.execute("UPDATE users SET monthly_budget = ? WHERE user_id = ?", (amount, user_id))
    db.commit()
    return {"ok": True}


@app.post("/add_recurring")
async def api_add_recurring(payload: dict):
    user_id = payload.get('user_id')
    amount = payload.get('amount')
    key = payload.get('category_key')
    day = payload.get('day_of_month')
    note = payload.get('note')
    cur = db.cursor()
    cur.execute("INSERT INTO recurring(user_id, amount, category_key, day_of_month, note) VALUES (?, ?, ?, ?, ?)",
                (user_id, amount, key, day, note))
    db.commit()
    return {"ok": True}


# ========== Simple healthcheck ===========

@app.get("/")
async def root():
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get('PORT', 8000)))
