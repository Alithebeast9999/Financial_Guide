"""
Finance Assistant Telegram Bot (Webhook-ready)

Features implemented:
- Webhook server via python-telegram-bot (run_webhook)
- SQLite DB with SQLAlchemy (auto-create schemas)
- Set monthly budget -> generates category limits from screenshot percentages
- Add expense dialog with category keyboard, checks limits and warns
- Recurring expenses added automatically at start of month-day
- Daily reminders (09:00 Europe/Moscow) and weekly reports (Mon 10:00)
- History with delete buttons
- Notifications toggle
- Designed to run on Render as a Web Service
"""

import os
import logging
import asyncio
from datetime import datetime, date, timedelta
from typing import Dict, Any
import pytz

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, relationship

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler, ConversationHandler

# -----------------------
# Config & Logging
# -----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "<PUT_TOKEN>")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # e.g. https://yourapp.onrender.com/webhook
PORT = int(os.environ.get("PORT", "8443"))
TZ = pytz.timezone("Europe/Moscow")  # UTC+3

# -----------------------
# DB Setup
# -----------------------
BASE_DIR = os.path.dirname(__file__)
DB_FILE = os.environ.get("DATABASE_URL", f"sqlite:///{os.path.join(BASE_DIR, 'data.sqlite')}")
engine = create_engine(DB_FILE, connect_args={"check_same_thread": False} if "sqlite" in DB_FILE else {})
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, index=True)
    first_name = Column(String, nullable=True)
    monthly_budget = Column(Float, default=0.0)
    notifications_enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class CategoryLimit(Base):
    __tablename__ = "category_limits"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    category = Column(String, index=True)
    percent = Column(Float)
    amount = Column(Float)
    user = relationship("User", backref="category_limits")

class Expense(Base):
    __tablename__ = "expenses"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    category = Column(String, index=True)
    amount = Column(Float)
    comment = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    date = Column(Date, default=date.today)
    recurring_id = Column(Integer, ForeignKey("recurrings.id"), nullable=True)
    user = relationship("User", backref="expenses")

class Recurring(Base):
    __tablename__ = "recurrings"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    category = Column(String)
    amount = Column(Float)
    day_of_month = Column(Integer)
    comment = Column(String, nullable=True)
    active = Column(Boolean, default=True)
    user = relationship("User", backref="recurrings")

def init_db():
    Base.metadata.create_all(bind=engine)

# -----------------------
# Categories & percents
# -----------------------
CATEGORY_PERCENT = {
    "Аренда жилья": 35.0,
    "Продуктовая корзина": 15.0,
    "Комм. услуги": 5.0,
    "Связь": 3.0,
    "Транспорт": 5.0,
    "Личный уход": 2.0,
    "Медицина": 8.0,
    "Инвестиции": 5.0,
    "Подушка безопасности": 5.0,
    "Развлечения": 7.0,
    "Отдых - путешествия": 5.0,
    "Шопинг": 5.0,
}

# -----------------------
# Helpers
# -----------------------
def get_or_create_user(db, tg_user) -> User:
    user = db.query(User).filter(User.telegram_id == tg_user.id).first()
    if not user:
        user = User(telegram_id=tg_user.id, first_name=tg_user.first_name or "", monthly_budget=0.0)
        db.add(user); db.commit(); db.refresh(user)
    return user

def save_category_limits(db, user: User, monthly_budget: float):
    db.query(CategoryLimit).filter(CategoryLimit.user_id == user.id).delete()
    db.commit()
    for cat, pct in CATEGORY_PERCENT.items():
        amount = round(monthly_budget * pct / 100.0, 2)
        cl = CategoryLimit(user_id=user.id, category=cat, percent=pct, amount=amount)
        db.add(cl)
    db.commit()

def get_month_range(target_date: date):
    start = date(target_date.year, target_date.month, 1)
    if target_date.month == 12:
        end = date(target_date.year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(target_date.year, target_date.month + 1, 1) - timedelta(days=1)
    return start, end

def sum_expenses(db, user: User, start_date: date, end_date: date) -> float:
    rows = db.query(Expense).filter(Expense.user_id == user.id, Expense.date >= start_date, Expense.date <= end_date).all()
    return sum(r.amount for r in rows)

def sum_expenses_by_category(db, user: User, start_date: date, end_date: date):
    rows = db.query(Expense).filter(Expense.user_id == user.id, Expense.date >= start_date, Expense.date <= end_date).all()
    res = {}
    for r in rows:
        res[r.category] = res.get(r.category, 0.0) + r.amount
    return res

# -----------------------
# Bot logic & handlers
# -----------------------
(ASK_AMOUNT, ASK_CATEGORY, ASK_COMMENT) = range(3)
temp_flows: Dict[int, Dict[str, Any]] = {}

CATEGORY_KEYS = list(CATEGORY_PERCENT.keys())
CATEGORY_ROWS = [CATEGORY_KEYS[i:i+2] for i in range(0, len(CATEGORY_KEYS), 2)]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = SessionLocal(); get_or_create_user(db, update.effective_user); db.close()
    await update.message.reply_text("Привет! Я — финансовый помощник. /setbudget, /addexpense, /addrecurring, /report week|month, /history, /notifications on|off")

async def setbudget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = SessionLocal()
    user = get_or_create_user(db, update.effective_user)
    if not context.args:
        await update.message.reply_text("Использование: /setbudget <сумма>")
        db.close(); return
    try:
        amount = float("".join(context.args).replace(",", "."))
        user.monthly_budget = amount; db.add(user); db.commit()
        save_category_limits(db, user, amount)
        await update.message.reply_text(f"Бюджет установлен: {amount:.2f} ₽. Лимиты сохранены.")
    except Exception:
        await update.message.reply_text("Ошибка: укажите число, например /setbudget 60000")
    finally:
        db.close()

async def showlimits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = SessionLocal(); user = get_or_create_user(db, update.effective_user)
    limits = db.query(CategoryLimit).filter(CategoryLimit.user_id == user.id).all()
    if not limits:
        await update.message.reply_text("Лимиты не установлены. /setbudget <сумма>")
        db.close(); return
    lines = [f"Лимиты для {user.monthly_budget:.2f} ₽:"]
    for l in sorted(limits, key=lambda x: -x.percent):
        lines.append(f"{l.category}: {l.percent:.0f}% = {l.amount:.2f} ₽")
    await update.message.reply_text("\n".join(lines))
    db.close()

# Add expense flow
async def addexpense_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Введите сумму расхода (₽):")
    return ASK_AMOUNT

async def ask_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().replace(",", ".")
    try:
        amount = float(text)
    except:
        await update.message.reply_text("Неверный формат. Введите число, например 1500")
        return ASK_AMOUNT
    uid = update.effective_user.id
    temp_flows[uid] = {"amount": amount}
    keyboard = [[KeyboardButton(cat) for cat in row] for row in CATEGORY_ROWS]
    keyboard.append([KeyboardButton("Другая")])
    await update.message.reply_text("Выберите категорию:", reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True))
    return ASK_CATEGORY

async def ask_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    chosen = update.message.text.strip()
    if chosen.lower() == "другая":
        chosen = "Другие"
    temp_flows[uid]["category"] = chosen
    await update.message.reply_text("Комментарий (опционально):")
    return ASK_COMMENT

async def save_expense(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    comment = update.message.text.strip()
    data = temp_flows.get(uid)
    if not data:
        await update.message.reply_text("Сессия истекла. Повторите /addexpense.")
        return ConversationHandler.END
    amount = data["amount"]; category = data["category"]
    db = SessionLocal(); user = get_or_create_user(db, update.effective_user)
    exp = Expense(user_id=user.id, category=category, amount=amount, comment=comment, date=date.today())
    db.add(exp); db.commit(); db.refresh(exp)
    # check limits
    start, end = get_month_range(date.today())
    cat_sums = sum_expenses_by_category(db, user, start, end)
    cat_spent = cat_sums.get(category, 0.0)
    cl = db.query(CategoryLimit).filter(CategoryLimit.user_id == user.id, CategoryLimit.category == category).first()
    total_spent = sum_expenses(db, user, start, end)
    resp = f"Записано: {amount:.2f} ₽ в '{category}'."
    if cl and cat_spent > cl.amount:
        resp += f"\n⚠️ Превышен лимит {category}: {cat_spent:.2f} / {cl.amount:.2f} ₽."
    if user.monthly_budget and total_spent > user.monthly_budget:
        resp += f"\n⚠️ Превышен месячный бюджет: {total_spent:.2f} / {user.monthly_budget:.2f} ₽."
    await update.message.reply_text(resp)
    db.close(); temp_flows.pop(uid, None)
    return ConversationHandler.END

async def cancel_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    temp_flows.pop(update.effective_user.id, None)
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END

# Recurring
async def addrecurring_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Добавить регулярный расход в формате: сумма;категория;день(1-31);комментарий (пример:\n25000;Аренда жилья;1;Аренда)")
    return 0

async def save_recurring(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    parts = [p.strip() for p in txt.split(";")]
    if len(parts) < 3:
        await update.message.reply_text("Неверный формат.")
        return 0
    try:
        amount = float(parts[0].replace(",", "."))
        category = parts[1]; day = int(parts[2]); comment = parts[3] if len(parts)>3 else None
        if not (1 <= day <= 31): raise ValueError()
    except:
        await update.message.reply_text("Неверные данные.")
        return 0
    db = SessionLocal(); user = get_or_create_user(db, update.effective_user)
    rec = Recurring(user_id=user.id, category=category, amount=amount, day_of_month=day, comment=comment, active=True)
    db.add(rec); db.commit()
    await update.message.reply_text(f"Регулярный расход сохранён: {amount:.2f} ₽, {category}, день {day}")
    db.close(); return ConversationHandler.END

# Reports & history
async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if not args: 
        await update.message.reply_text("Использование: /report week или /report month"); return
    period = args[0].lower()
    db = SessionLocal(); user = get_or_create_user(db, update.effective_user)
    today = date.today()
    if period == "week":
        start = today - timedelta(days=today.weekday()); end = start + timedelta(days=6)
    elif period == "month":
        start, end = get_month_range(today)
    else:
        await update.message.reply_text("Период: week или month"); db.close(); return
    total = sum_expenses(db, user, start, end)
    by_cat = sum_expenses_by_category(db, user, start, end)
    lines = [f"Отчёт {start}—{end}", f"Итого: {total:.2f} ₽", "По категориям:"]
    for cat, amt in sorted(by_cat.items(), key=lambda x:-x[1]):
        lines.append(f"{cat}: {amt:.2f} ₽")
    await update.message.reply_text("\n".join(lines)); db.close()

async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = SessionLocal(); user = get_or_create_user(db, update.effective_user)
    start, _ = get_month_range(date.today())
    rows = db.query(Expense).filter(Expense.user_id==user.id, Expense.date>=start).order_by(Expense.created_at.desc()).limit(20).all()
    if not rows:
        await update.message.reply_text("Нет трат."); db.close(); return
    for r in rows:
        text = f"{r.id}. {r.date} — {r.category}: {r.amount:.2f} ₽\n{r.comment or ''}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton('Удалить ❌', callback_data=f'del_exp:{r.id}')]])
        await update.message.reply_text(text, reply_markup=kb)
    db.close()

async def delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    data = q.data or ""
    if data.startswith("del_exp:"):
        exp_id = int(data.split(":",1)[1])
        db = SessionLocal(); exp = db.query(Expense).filter(Expense.id==exp_id).first()
        if not exp:
            await q.edit_message_text("Не найдено.")
            db.close(); return
        # ensure owner
        user = db.query(User).filter(User.id==exp.user_id).first()
        if user and user.telegram_id != q.from_user.id:
            await q.edit_message_text("Нет доступа."); db.close(); return
        db.delete(exp); db.commit(); db.close()
        await q.edit_message_text("Удалено.")

async def notifications_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = SessionLocal(); user = get_or_create_user(db, update.effective_user)
    if not context.args:
        await update.message.reply_text(f"Уведомления: {'ON' if user.notifications_enabled else 'OFF'}. /notifications on|off"); db.close(); return
    a = context.args[0].lower()
    user.notifications_enabled = a in ('on','1','yes','true')
    db.add(user); db.commit(); await update.message.reply_text(f"Уведомления {'включены' if user.notifications_enabled else 'выключены'}"); db.close()

# Scheduler jobs
async def daily_reminder_job(app):
    db = SessionLocal(); users = db.query(User).filter(User.notifications_enabled==True).all(); db.close()
    for u in users:
        try:
            await app.bot.send_message(chat_id=u.telegram_id, text="Напоминание: добавьте сегодняшние траты /addexpense")
        except Exception as e:
            logger.warning("daily reminder failed %s", e)

async def weekly_report_job(app):
    db = SessionLocal(); users = db.query(User).all()
    today = date.today(); start = today - timedelta(days=today.weekday()); end = start + timedelta(days=6)
    for u in users:
        try:
            total = sum_expenses(db, u, start, end)
            by_cat = sum_expenses_by_category(db, u, start, end)
            lines = [f"Авто-отчёт {start}—{end}", f"Итого: {total:.2f} ₽"]
            for cat, amt in sorted(by_cat.items(), key=lambda x:-x[1])[:5]:
                lines.append(f"{cat}: {amt:.2f} ₽")
            await app.bot.send_message(chat_id=u.telegram_id, text="\n".join(lines))
        except Exception as e:
            logger.warning("weekly report failed %s", e)
    db.close()

async def recurring_job(app):
    db = SessionLocal(); today = datetime.now(TZ).date(); day = today.day
    recs = db.query(Recurring).filter(Recurring.active==True).all()
    for r in recs:
        start, end = get_month_range(today); last_day = end.day; target_day = min(r.day_of_month, last_day)
        if day == target_day:
            existing = db.query(Expense).filter(Expense.recurring_id==r.id, Expense.date>=start, Expense.date<=end).first()
            if existing: continue
            exp = Expense(user_id=r.user_id, category=r.category, amount=r.amount, comment=f"Recurring: {r.comment or ''}", date=today, recurring_id=r.id)
            db.add(exp); db.commit()
            try:
                user = db.query(User).filter(User.id==r.user_id).first()
                if user:
                    await app.bot.send_message(chat_id=user.telegram_id, text=f"Добавлен регулярный расход {r.amount:.2f} ₽ → {r.category}")
            except Exception:
                logger.exception("notify recurring failed")
    db.close()

# Build app & handlers
def build_app():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setbudget", setbudget))
    app.add_handler(CommandHandler("showlimits", showlimits))
    app.add_handler(CommandHandler("report", report_cmd))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("notifications", notifications_cmd))
    app.add_handler(CommandHandler("addrecurring", addrecurring_cmd))

    rec_conv = ConversationHandler(entry_points=[CommandHandler("addrecurring", addrecurring_cmd)],
                                   states={0: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_recurring)]},
                                   fallbacks=[CommandHandler("cancel", cancel_flow)])
    app.add_handler(rec_conv)

    conv = ConversationHandler(entry_points=[CommandHandler("addexpense", addexpense_cmd)],
                               states={
                                   ASK_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_category)],
                                   ASK_CATEGORY: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_comment)],
                                   ASK_COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_expense)],
                               },
                               fallbacks=[CommandHandler("cancel", cancel_flow)])
    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(delete_callback, pattern=r"^del_exp:"))
    return app

async def start_scheduler(app):
    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.add_job(lambda: asyncio.create_task(daily_reminder_job(app)), CronTrigger(hour=9, minute=0, timezone=TZ))
    scheduler.add_job(lambda: asyncio.create_task(weekly_report_job(app)), CronTrigger(day_of_week="mon", hour=10, minute=0, timezone=TZ))
    scheduler.add_job(lambda: asyncio.create_task(recurring_job(app)), CronTrigger(hour=0, minute=5, timezone=TZ))
    scheduler.start()
    logger.info("Scheduler started")

def main():
    init_db()
    app = build_app()
    async def _on_startup():
        if WEBHOOK_URL:
            try:
                await app.bot.set_webhook(WEBHOOK_URL)
                logger.info("Webhook set: %s", WEBHOOK_URL)
            except Exception:
                logger.exception("set_webhook failed")
        await start_scheduler(app)
    webhook_path = "/webhook"
    listen_addr = "0.0.0.0"
    app.run_webhook(listen=listen_addr, port=PORT, url_path=webhook_path, webhook_url=WEBHOOK_URL, on_startup=_on_startup)

if __name__ == "__main__":
    main()
