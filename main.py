# main.py
import logging
import sqlite3
import os
import re
from datetime import datetime, timezone, timedelta, time, date
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
)

# Basic config
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Environment
TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
BASE_URL = os.environ.get("BASE_URL")  # https://your-render-url
PORT = int(os.environ.get("PORT", "8080"))
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{BASE_URL}{WEBHOOK_PATH}" if BASE_URL else None

# Timezone used for scheduling
TZ = timezone(timedelta(hours=3))  # UTC+3 fixed offset (as requested)

# Categories and recommended percents (from provided image)
CATEGORIES = {
    # "section": (category_name, percent)
    "Аренда жилья": 35,
    "Продуктовая корзина": 15,
    "Комм. услуги": 5,
    "Связь": 3,
    "Транспорт": 5,
    "Личный уход": 2,
    "Медицина": 8,
    "Инвестиции": 5,
    "Подушка безопасности": 5,
    "Развлечения": 7,
    "Отдых - путешествия": 5,
    "Шопинг": 5,
}

# DB file
DB_PATH = os.environ.get("DB_PATH", "data.sqlite")

# Conversation states
ASKING_INCOME = "ASKING_INCOME"
AWAITING_EXPENSE_AMOUNT = "AWAITING_EXPENSE_AMOUNT"
AWAITING_EXPENSE_CATEGORY = "AWAITING_EXPENSE_CATEGORY"
AWAITING_RECURRING_PARAMS = "AWAITING_RECURRING_PARAMS"

# --- Database helpers -------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        chat_id INTEGER UNIQUE,
        income INTEGER DEFAULT 0,
        notify INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    )
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS expenses (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        amount INTEGER,
        category TEXT,
        note TEXT,
        ts TIMESTAMP,
        recurring_id INTEGER,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """
    )
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS recurring (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        amount INTEGER,
        category TEXT,
        day_of_month INTEGER,
        active INTEGER DEFAULT 1
    )
    """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_user ON expenses(user_id)")
    conn.commit()
    conn.close()


def get_db():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    return conn


# --- Business logic --------------------------------------------------
def calculate_limits(income: int) -> Dict[str, Dict[str, Any]]:
    """
    Returns dict of category -> {percent, limit_amount}
    """
    result = {}
    for cat, pct in CATEGORIES.items():
        result[cat] = {"percent": pct, "limit": int(income * pct / 100)}
    return result


def ensure_user(chat_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE chat_id = ?", (chat_id,))
    row = cur.fetchone()
    if not row:
        cur.execute("INSERT INTO users (chat_id) VALUES (?)", (chat_id,))
        conn.commit()
        user_id = cur.lastrowid
    else:
        user_id = row["id"]
    conn.close()
    return user_id


def set_income(chat_id: int, income: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE users SET income = ? WHERE chat_id = ?", (income, chat_id))
    conn.commit()
    conn.close()


def get_user_by_chat(chat_id: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE chat_id = ?", (chat_id,))
    row = cur.fetchone()
    conn.close()
    return row


def add_expense_db(user_id: int, amount: int, category: str, note: Optional[str] = None, recurring_id: Optional[int] = None):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO expenses (user_id, amount, category, note, ts, recurring_id) VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, amount, category, note, datetime.now(TZ), recurring_id),
    )
    conn.commit()
    eid = cur.lastrowid
    conn.close()
    return eid


def delete_expense_by_id(user_id: int, expense_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM expenses WHERE id = ? AND user_id = ?", (expense_id, user_id))
    changed = cur.rowcount
    conn.commit()
    conn.close()
    return changed > 0


def list_expenses(user_id: int, limit: int = 50) -> List[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, amount, category, note, ts FROM expenses WHERE user_id = ? ORDER BY ts DESC LIMIT ?",
        (user_id, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_expenses_sum_by_category(user_id: int, since: Optional[datetime] = None) -> Dict[str, int]:
    conn = get_db()
    cur = conn.cursor()
    if since:
        cur.execute(
            "SELECT category, SUM(amount) as s FROM expenses WHERE user_id = ? AND ts >= ? GROUP BY category",
            (user_id, since),
        )
    else:
        cur.execute("SELECT category, SUM(amount) as s FROM expenses WHERE user_id = ? GROUP BY category", (user_id,))
    rows = cur.fetchall()
    conn.close()
    return {r["category"]: (r["s"] or 0) for r in rows}


def add_recurring(user_id: int, amount: int, category: str, day_of_month: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO recurring (user_id, amount, category, day_of_month, active) VALUES (?, ?, ?, ?, 1)",
        (user_id, amount, category, day_of_month),
    )
    conn.commit()
    rid = cur.lastrowid
    conn.close()
    return rid


def list_recurring(user_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, amount, category, day_of_month, active FROM recurring WHERE user_id = ?", (user_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


def apply_recurring_for_day(day: int):
    """
    Create expenses for all recurring where day_of_month == day or day == last day of month
    This is called daily by scheduler.
    """
    conn = get_db()
    cur = conn.cursor()
    # Get all recurring active
    cur.execute("SELECT id, user_id, amount, category, day_of_month FROM recurring WHERE active = 1")
    rows = cur.fetchall()
    inserted = []
    for r in rows:
        rid, user_id, amount, category, dom = r
        if dom == day:
            cur.execute(
                "INSERT INTO expenses (user_id, amount, category, note, ts, recurring_id) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, amount, category, "Авто: регулярный платеж", datetime.now(TZ), rid),
            )
            inserted.append((user_id, rid))
    conn.commit()
    conn.close()
    return inserted


# --- Telegram handlers ------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_user(chat_id)
    kb = [
        [KeyboardButton("Добавить трату"), KeyboardButton("История")],
        [KeyboardButton("Моя статистика"), KeyboardButton("Помощь /commands")],
    ]
    text = (
        "Привет! Я — финансовый помощник.\n\n"
        "Я помогу учесть и оптимизировать ваши расходы, напомню про регулярные платежи, "
        "уведомлю если лимит категории будет превышен и пришлю еженедельные/ежемесячные отчёты.\n\n"
        "Пожалуйста, введите ваш месячный доход в рублях (например: 100000)."
    )
    await update.message.reply_text(text, reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))
    return ASKING_INCOME


async def handle_income(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = update.message.text.strip().replace(" ", "")
    if not text.isdigit():
        await update.message.reply_text("Пожалуйста, отправьте сумму цифрами, например: 100000")
        return ASKING_INCOME
    income = int(text)
    set_income(chat_id, income)
    limits = calculate_limits(income)
    # reply with summary
    lines = [f"Установлен месячный доход: {income}₽\nРекомендованные лимиты по категориям:"]
    for cat, v in limits.items():
        lines.append(f"- {cat}: {v['percent']}% → {v['limit']}₽")
    await update.message.reply_text("\n".join(lines), reply_markup=ReplyKeyboardRemove())
    # Offer quick actions
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Добавить трату", callback_data="btn_add")],
            [InlineKeyboardButton("Моя статистика", callback_data="btn_stats"), InlineKeyboardButton("История", callback_data="btn_history")],
            [InlineKeyboardButton("Помощь", callback_data="btn_help")],
        ]
    )
    await update.message.reply_text("Чем хотите заняться дальше?", reply_markup=kb)
    return ConversationHandler.END


async def button_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if data == "btn_add":
        await q.message.reply_text("Отправьте сумму траты (например: 2500). Можно прямо сейчас добавить: `2500`", parse_mode="Markdown")
        return
    if data == "btn_history":
        await send_history(q.message, context)
        return
    if data == "btn_stats":
        await send_stats(q.message, context)
        return
    if data == "btn_help":
        await send_help(q.message, context)
        return


async def send_help(dest, context):
    text = (
        "Команды:\n"
        "/start - начать\n"
        "/report week - отчёт за неделю\n"
        "/report month - отчёт за месяц\n"
        "/recurring - добавить регулярный платёж (формат: /recurring сумма|категория|день_месяца)\n\n"
        "Быстрые кнопки:\n"
        "- Добавить трату (можно просто отправить число)\n"
        "- История (удалить запись — красный крестик)\n"
        "- Моя статистика (показывает текущие траты и лимиты)\n\n"
        "Если отправите сумму (например: `5000`), бот предложит выбрать категорию и сохранит трату."
    )
    await dest.reply_text(text)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    chat_id = update.effective_chat.id
    # If message is just a number, treat as expense amount
    normalized = re.sub(r"[^\d]", "", text)
    if normalized.isdigit():
        amount = int(normalized)
        # Ask category
        context.user_data["pending_amount"] = amount
        # inline keyboard of categories
        buttons = []
        row = []
        for i, cat in enumerate(CATEGORIES.keys()):
            row.append(InlineKeyboardButton(cat, callback_data=f"cat|{cat}"))
            if (i + 1) % 2 == 0:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([InlineKeyboardButton("Отмена", callback_data="cat|CANCEL")])
        await update.message.reply_text(f"Сумма: {amount}₽\nВыберите категорию:", reply_markup=InlineKeyboardMarkup(buttons))
        return
    # other commands or phrases
    text_lower = text.lower()
    if text_lower in ["добавить трату", "add expense", "add"]:
        await update.message.reply_text("Отправьте сумму (например: 1200) или: `1200 groceries` (опционно с категорией в одной строке).")
        return
    if text_lower in ["история", "history"]:
        await send_history(update.message, context)
        return
    if text_lower in ["моя статистика", "статистика"]:
        await send_stats(update.message, context)
        return
    if text_lower in ["помощь", "/help", "/commands"]:
        await send_help(update.message, context)
        return
    # Fallback
    await update.message.reply_text("Не понял. Используйте кнопки или /commands для списка команд.")


async def cat_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    _, cat = data.split("|", 1)
    if cat == "CANCEL":
        await q.message.reply_text("Операция отменена.")
        context.user_data.pop("pending_amount", None)
        return
    amount = context.user_data.get("pending_amount")
    if not amount:
        # Maybe user passed "1200 groceries" earlier
        await q.message.reply_text("Не найдена сумма. Отправьте сумму, например: 1200")
        return
    chat_id = q.message.chat.id
    user = get_user_by_chat(chat_id)
    if not user:
        ensure_user(chat_id)
        user = get_user_by_chat(chat_id)
    add_expense_db(user["id"], amount, cat)
    context.user_data.pop("pending_amount", None)
    await q.message.reply_text(f"Записано: {amount}₽ — {cat}")
    # Check limit
    await check_limits_and_warn(q.message, user["id"], cat, context)


async def check_limits_and_warn(dest_message, user_id: int, category: str, context: ContextTypes.DEFAULT_TYPE):
    user = None
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT income, chat_id FROM users WHERE id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return
    income = row["income"] or 0
    chat_id = row["chat_id"]
    limits = calculate_limits(income)
    cat_limit = limits.get(category, {}).get("limit", 0)
    # sum current month for the category
    now = datetime.now(TZ)
    start_month = datetime(now.year, now.month, 1, tzinfo=TZ)
    sums = get_expenses_sum_by_category(user_id, since=start_month)
    cat_sum = sums.get(category, 0)
    # overall month sum
    overall = sum(sums.values())
    total_limit = income
    warn_msgs = []
    if cat_limit and cat_sum > cat_limit:
        warn_msgs.append(f"⚠️ Вы превысили лимит для категории *{category}*: {cat_sum}₽ / {cat_limit}₽.")
    if total_limit and overall > total_limit:
        warn_msgs.append(f"⚠️ Общие расходы за месяц превысили доход: {overall}₽ / {total_limit}₽.")
    if warn_msgs:
        await dest_message.reply_text("\n".join(warn_msgs))


async def send_history(dest_message, context):
    chat_id = dest_message.chat.id
    user = get_user_by_chat(chat_id)
    if not user:
        await dest_message.reply_text("Сначала укажите доход через /start.")
        return
    rows = list_expenses(user["id"], limit=20)
    if not rows:
        await dest_message.reply_text("История пуста.")
        return
    for r in rows:
        ts = datetime.fromisoformat(r["ts"]).astimezone(TZ).strftime("%Y-%m-%d %H:%M")
        text = f"{r['id']}: {r['amount']}₽ — {r['category']} ({ts})"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Удалить", callback_data=f"del|{r['id']}")]])
        await dest_message.reply_text(text, reply_markup=kb)


async def delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    _, eid = data.split("|", 1)
    chat_id = q.message.chat.id
    user = get_user_by_chat(chat_id)
    if not user:
        await q.message.reply_text("Пользователь не найден.")
        return
    ok = delete_expense_by_id(user["id"], int(eid))
    if ok:
        await q.message.reply_text("Запись удалена.")
    else:
        await q.message.reply_text("Не удалось удалить запись.")


async def send_stats(dest_message, context):
    chat_id = dest_message.chat.id
    user = get_user_by_chat(chat_id)
    if not user:
        await dest_message.reply_text("Сначала укажите доход через /start.")
        return
    income = user["income"] or 0
    limits = calculate_limits(income)
    now = datetime.now(TZ)
    start_month = datetime(now.year, now.month, 1, tzinfo=TZ)
    sums = get_expenses_sum_by_category(user["id"], since=start_month)
    lines = [f"Статистика за текущий месяц (до {now.strftime('%Y-%m-%d')}):", f"Доход: {income}₽\n"]
    total_spent = 0
    for cat, v in limits.items():
        spent = sums.get(cat, 0)
        total_spent += spent
        pct = (spent / v["limit"] * 100) if v["limit"] > 0 else 0
        lines.append(f"{cat}: {spent}₽ / {v['limit']}₽ ({v['percent']}%) — {int(pct)}% от лимита")
    lines.append(f"\nИтого потрачено: {total_spent}₽")
    await dest_message.reply_text("\n".join(lines))


async def recurring_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Expected formats:
    /recurring 25000|Аренда жилья|1
    or just /recurring and bot will reply instructions.
    """
    chat_id = update.effective_chat.id
    args = update.message.text.replace("/recurring", "").strip()
    if not args:
        await update.message.reply_text(
            "Добавить регулярный платёж — формат:\n"
            "/recurring сумма|категория|день_месяца\n"
            "Пример:\n/recurring 35000|Аренда жилья|1\n\n"
            "Список доступных категорий:\n" + ", ".join(CATEGORIES.keys())
        )
        return
    parts = [p.strip() for p in args.split("|")]
    if len(parts) != 3 or not parts[0].isdigit() or not parts[2].isdigit():
        await update.message.reply_text("Неправильный формат. Пример: /recurring 35000|Аренда жилья|1")
        return
    amount = int(parts[0])
    category = parts[1]
    day = int(parts[2])
    chat_user = get_user_by_chat(chat_id)
    if not chat_user:
        ensure_user(chat_id)
        chat_user = get_user_by_chat(chat_id)
    if category not in CATEGORIES:
        await update.message.reply_text("Неизвестная категория. Используйте одну из: " + ", ".join(CATEGORIES.keys()))
        return
    rid = add_recurring(chat_user["id"], amount, category, day)
    await update.message.reply_text(f"Регулярный платёж добавлен (id {rid}): {amount}₽ — {category} каждый {day}-й день.")


async def report_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = update.message.text.strip()
    if text.startswith("/report"):
        parts = text.split()
        if len(parts) == 1 or parts[1] not in ("week", "month"):
            await update.message.reply_text("Используйте: /report week или /report month")
            return
        period = parts[1]
        await send_report_for_user(chat_id, period, context)


async def send_report_for_user(chat_id: int, period: str, context: ContextTypes.DEFAULT_TYPE):
    user = get_user_by_chat(chat_id)
    if not user:
        return
    now = datetime.now(TZ)
    if period == "week":
        since = now - timedelta(days=7)
    else:
        # month
        since = datetime(now.year, now.month, 1, tzinfo=TZ)
    sums = get_expenses_sum_by_category(user["id"], since=since)
    total = sum(sums.values())
    lines = [f"Отчёт ({period}) до {now.strftime('%Y-%m-%d')}", f"Всего потрачено: {total}₽", ""]
    for cat, amt in sums.items():
        lines.append(f"- {cat}: {amt}₽")
    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))


# --- Scheduled jobs ----------------------------------------------------
async def daily_reminder(context: ContextTypes.DEFAULT_TYPE):
    """
    Send daily reminder to users with notify=1 to add today's expenses.
    """
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT chat_id FROM users WHERE notify = 1")
    rows = cur.fetchall()
    conn.close()
    for r in rows:
        chat_id = r["chat_id"]
        try:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("Добавить трату", callback_data="btn_add")]])
            await context.bot.send_message(chat_id=chat_id, text="Напоминание: не забудьте добавить сегодняшние траты.", reply_markup=kb)
        except Exception as e:
            logger.exception("Failed to send daily reminder to %s: %s", chat_id, e)


async def weekly_autoreport(context: ContextTypes.DEFAULT_TYPE):
    """
    Send weekly report to users with notify=1 every Monday (UTC+3).
    """
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT chat_id, id FROM users WHERE notify = 1")
    rows = cur.fetchall()
    conn.close()
    for r in rows:
        chat_id = r["chat_id"]
        user_id = r["id"]
        # compose report for last 7 days
        now = datetime.now(TZ)
        since = now - timedelta(days=7)
        sums = get_expenses_sum_by_category(user_id, since=since)
        total = sum(sums.values())
        lines = [f"Еженедельный отчёт (последние 7 дней):\nВсего: {total}₽\n"]
        for cat, amt in sums.items():
            lines.append(f"- {cat}: {amt}₽")
        try:
            await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
        except Exception as e:
            logger.exception("Failed to send weekly report to %s: %s", chat_id, e)


async def apply_recurring_job(context: ContextTypes.DEFAULT_TYPE):
    # Called daily; determine today's day-of-month and apply recurrings
    now = datetime.now(TZ)
    today = now.day
    inserted = apply_recurring_for_day(today)
    # for each insertion we may want to notify user
    for user_id, rid in inserted:
        # lookup chat_id
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        conn.close()
        if row:
            try:
                await context.bot.send_message(row["chat_id"], "Добавлен регулярный платёж автоматом.")
            except Exception:
                logger.exception("Notify recurring failed for user %s", user_id)


# --- Main setup -------------------------------------------------------
async def on_startup(application):
    # Set webhook if BASE_URL provided
    if WEBHOOK_URL:
        await application.bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    # schedule daily reminder at 09:00 UTC+3
    # Using JobQueue (times with tzinfo)
    job_queue = application.job_queue
    # daily reminder at 09:00
    job_queue.run_daily(daily_reminder, time(hour=9, tzinfo=TZ), name="daily_reminder")
    # weekly autoreport every Monday at 09:30 UTC+3
    # weekday: Monday is 0, but run_daily has "days" args optional. Use run_repeating with next interval = 7 days starting next Monday.
    job_queue.run_daily(weekly_autoreport, time(hour=9, minute=30, tzinfo=TZ), name="weekly_report", days=(0,))
    # recurring application job: run daily at 00:05 UTC+3
    job_queue.run_daily(apply_recurring_job, time(hour=0, minute=5, tzinfo=TZ), name="apply_recurring")
    logger.info("Jobs scheduled.")


async def on_shutdown(application):
    # remove webhook
    if WEBHOOK_URL:
        await application.bot.delete_webhook()
        logger.info("Webhook deleted.")


def build_application():
    if not TOKEN:
        logger.error("TELEGRAM_TOKEN is not set in environment.")
        raise RuntimeError("TELEGRAM_TOKEN env var required")
    application = ApplicationBuilder().token(TOKEN).build()

    # Conversation for start/income
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            ASKING_INCOME: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_income)],
        },
        fallbacks=[],
    )
    application.add_handler(conv_handler)

    # Buttons
    application.add_handler(CallbackQueryHandler(button_router, pattern=r"^btn_"))
    # category selection
    application.add_handler(CallbackQueryHandler(cat_selected, pattern=r"^cat\|"))
    # delete
    application.add_handler(CallbackQueryHandler(delete_callback, pattern=r"^del\|"))

    # text handler (for amounts, quick phrases)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # recurring command
    application.add_handler(CommandHandler("recurring", recurring_command))

    # reports
    application.add_handler(CommandHandler("report", report_handler))
    application.add_handler(MessageHandler(filters.Regex(r"^/report (week|month)$"), report_handler))

    # help/shorthand
    application.add_handler(CommandHandler("help", lambda u, c: send_help(u.message, c)))

    # startup/shutdown hooks
    application.post_init = on_startup
    application.post_shutdown = on_shutdown

    return application


def main():
    # init db
    init_db()
    application = build_application()

    # Run as webhook server (built-in) so Render can route HTTP -> this process
    # Listen on 0.0.0.0:PORT
    listen = "0.0.0.0"
    logger.info("Starting webhook server on %s:%s", listen, PORT)
    # path is WEBHOOK_PATH, and url must be BASE_URL + WEBHOOK_PATH
    if WEBHOOK_URL:
        application.run_webhook(
            listen=listen,
            port=PORT,
            webhook_path=WEBHOOK_PATH,
            webhook_url_path=WEBHOOK_PATH,
            webhook_url=WEBHOOK_URL,
        )
    else:
        # If no BASE_URL provided run long-polling (useful for local dev)
        logger.warning("BASE_URL not set — running polling mode (dev only).")
        application.run_polling()


if __name__ == "__main__":
    main()
