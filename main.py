#!/usr/bin/env python3
# main.py
"""
Telegram Finance Assistant Bot
Designed to run as a Render Web Service using webhook.
Features:
 - /start, /help
 - prompting user for monthly income and computing recommended category limits (see screenshot structure)
 - add expense by button or by sending a number (will ask category)
 - history with inline delete (red cross)
 - persistent sqlite (SQLAlchemy)
 - recurring expenses (recorded automatically on the configured day)
 - daily reminder (UTC+3) to add today's expenses for users with notifications enabled
 - weekly automatic report (Monday UTC+3)
 - commands: /report week, /report month
"""

import os
import logging
import asyncio
from datetime import datetime, date, timedelta, time
from typing import Optional, List, Dict, Tuple

from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Date, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from telegram.constants import ParseMode as PM
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes, ApplicationBuilder

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.environ.get("TELEGRAM_TOKEN")
APP_URL = os.environ.get("APP_URL")  # e.g. https://your-app.onrender.com
if not TOKEN:
    logger.error("TELEGRAM_TOKEN not set in env")
    raise RuntimeError("TELEGRAM_TOKEN not set")
if not APP_URL:
    logger.warning("APP_URL not set. Webhook setup will be skipped (useful for local testing).")

# DB setup
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///data.sqlite")
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()

# --- Models ---
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)  # telegram id
    username = Column(String, nullable=True)
    monthly_income = Column(Float, default=0.0)
    currency = Column(String, default="₽")
    notify = Column(Boolean, default=True)
    locale = Column(String, default="ru")
    created_at = Column(DateTime, default=datetime.utcnow)
    # recommended limits stored as JSON-like string "category:percent,..." (simple)
    limits = Column(Text, default="")  

    expenses = relationship("Expense", back_populates="user", cascade="all, delete-orphan")
    recurrings = relationship("Recurring", back_populates="user", cascade="all, delete-orphan")

class Expense(Base):
    __tablename__ = "expenses"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    amount = Column(Float)
    category = Column(String)
    note = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="expenses")

class Recurring(Base):
    __tablename__ = "recurrings"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    amount = Column(Float)
    category = Column(String)
    day = Column(Integer)  # day of month to auto-add
    active = Column(Boolean, default=True)
    note = Column(String, nullable=True)

    user = relationship("User", back_populates="recurrings")

Base.metadata.create_all(bind=engine)

# --- Category config based on screenshot (percentages) ---
# Groups: "NADO" (must), "MOGU" (can), "HOCHU" (want)
CATEGORY_PERCENT = {
    # NADO
    "аренда жилья": 35,
    "продуктовая корзина": 15,
    "комм. услуги": 5,
    "связь": 3,
    "транспорт": 5,
    "личный уход": 2,
    "медицина": 8,
    # MOGU
    "инвестиции": 5,
    "подушка безопасности": 5,
    # HOCHU
    "развлечения": 7,
    "отдых - путешествия": 5,
    "шопинг": 5,
}

# Helper: default categories list for keyboards
CATEGORIES = list(CATEGORY_PERCENT.keys())

# Bot & FastAPI
bot = Bot(token=TOKEN)
app = FastAPI()

# Dispatcher for convenience (we'll create handlers manually)
# We'll use python-telegram-bot Application for updates handling in webhook endpoint
application = ApplicationBuilder().token(TOKEN).build()

# Scheduler
scheduler = AsyncIOScheduler()
scheduler.start()

# Utility DB helpers
def get_user(db, user_id: int) -> Optional[User]:
    return db.query(User).filter(User.id == user_id).first()

def create_user_if_missing(db, tg_user) -> User:
    user = get_user(db, tg_user.id)
    if not user:
        user = User(id=tg_user.id, username=tg_user.username or tg_user.full_name)
        db.add(user)
        db.commit()
        db.refresh(user)
    return user

def save_limits_for_user(db, user: User):
    """Compute recommended limits from CATEGORY_PERCENT and user's income"""
    if user.monthly_income is None or user.monthly_income <= 0:
        return
    parts = []
    for cat, pct in CATEGORY_PERCENT.items():
        amount = round(user.monthly_income * pct / 100.0, 2)
        parts.append(f"{cat}:{pct}:{amount}")
    user.limits = ",".join(parts)
    db.add(user)
    db.commit()

def parse_limits(limits_str: str) -> Dict[str, Tuple[int, float]]:
    """Return dict cat -> (percent, amount)"""
    out = {}
    if not limits_str:
        return out
    for part in limits_str.split(","):
        try:
            cat, pct, amt = part.split(":", 2)
            out[cat] = (int(pct), float(amt))
        except Exception:
            continue
    return out

# --- Telegram utilities / keyboards ---
def main_keyboard():
    kb = [
        [KeyboardButton("➕ Добавить трату"), KeyboardButton("📈 Моя статистика")],
        [KeyboardButton("🕘 История"), KeyboardButton("❓ Помощь / Команды")]
    ]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def categories_inline():
    buttons = []
    for c in CATEGORIES:
        buttons.append([InlineKeyboardButton(c, callback_data=f"cat|{c}")])
    return InlineKeyboardMarkup(buttons)

# --- Handlers ---
async def start_handler(update: Update, context):
    db = SessionLocal()
    try:
        tg_user = update.effective_user
        user = create_user_if_missing(db, tg_user)
        text = (
            "Привет! Я — твой финансовый помощник.\n\n"
            "Я помогу отслеживать расходы, рассчитывать рекомендуемые лимиты по категориям (НАДО/МОГУ/ХОЧУ), "
            "напоминать про регулярные платежи и отправлять отчёты.\n\n"
            "Пожалуйста, введи свой ежемесячный доход в рублях (например: 50 000 или 50000). Валюта — ₽.\n\n"
            "Также ты можешь воспользоваться кнопками внизу."
        )
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, reply_markup=main_keyboard())
    finally:
        db.close()

async def help_handler(update: Update, context):
    text = (
        "Команды и функции:\n"
        "/start — перезапустить приветствие\n"
        "/help — эта подсказка\n"
        "/report week — недельный отчёт\n"
        "/report month — месячный отчёт\n\n"
        "Кнопки:\n"
        "➕ Добавить трату — добавить сумму и категорию\n"
        "📈 Моя статистика — покажет текущие расходы и ограничения\n"
        "🕘 История — список трат с возможностью удалить\n"
        "❓ Помощь / Команды — это сообщение\n\n"
        "Если прислать просто число (например, 350), бот попросит указать категорию и потом сохранит трату."
    )
    await context.bot.send_message(chat_id=update.effective_chat.id, text=text)

async def message_handler(update: Update, context):
    """
    Handle free-form messages:
    - If user has no monthly_income set and message looks like a number -> set income
    - If message is numeric -> treat as expense amount and prompt for category
    - If message is regular text -> react to buttons
    """
    db = SessionLocal()
    try:
        text = (update.message.text or "").strip()
        user = create_user_if_missing(db, update.effective_user)

        # Try parse number from message (allow spaces)
        cleaned = text.replace(" ", "").replace(",", ".")
        is_number = False
        try:
            val = float(cleaned)
            is_number = True
        except Exception:
            is_number = False

        if (user.monthly_income is None or user.monthly_income == 0.0) and is_number:
            # set monthly income
            user.monthly_income = float(val)
            save_limits_for_user(db, user)
            await context.bot.send_message(chat_id=update.effective_chat.id,
                                           text=f"Ок, я сохранил твой месячный доход: {user.monthly_income:.2f} ₽.\nЯ также рассчитал рекомендованные лимиты по категориям.",
                                           reply_markup=main_keyboard())
            return

        # If numeric -> treat as expense amount and ask category
        if is_number:
            amount = float(val)
            # save a temporary context in-memory: store in application.chat_data
            chat_data = context.chat_data
            chat_data["pending_amount"] = amount
            await context.bot.send_message(chat_id=update.effective_chat.id,
                                           text=f"Вы хотите записать трату {amount:.2f} ₽. Выберите категорию:",
                                           reply_markup=categories_inline())
            return

        # Handle buttons by label
        if text.lower().startswith("➕") or "добав" in text.lower():
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Введите сумму траты (например: 350) или выберите категорию и сумму:", reply_markup=categories_inline())
            return

        if "истор" in text.lower() or "🕘" in text:
            # list last 20 expenses
            exps = db.query(Expense).filter(Expense.user_id == user.id).order_by(Expense.created_at.desc()).limit(20).all()
            if not exps:
                await context.bot.send_message(chat_id=update.effective_chat.id, text="История пуста.")
                return
            for e in exps:
                created = e.created_at.strftime("%Y-%m-%d %H:%M")
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Удалить", callback_data=f"del|{e.id}")]])
                await context.bot.send_message(chat_id=update.effective_chat.id, text=f"{e.amount:.2f} ₽ | {e.category}\n{e.note or ''}\n{created}", reply_markup=kb)
            return

        if "статист" in text.lower() or "📈" in text:
            await send_statistics(user, context)
            return

        if "помощ" in text.lower() or "команд" in text.lower() or "❓" in text:
            await help_handler(update, context)
            return

        # default reply
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Не понял. Используйте кнопки или /help.")
    finally:
        db.close()

async def callback_handler(update: Update, context):
    """Handle inline callbacks: category selection and deletion"""
    db = SessionLocal()
    try:
        query = update.callback_query
        await query.answer()
        data = query.data or ""
        user = create_user_if_missing(db, query.from_user)
        if data.startswith("cat|"):
            cat = data.split("|", 1)[1]
            # check if pending_amount in chat_data
            amount = context.chat_data.pop("pending_amount", None)
            if amount is None:
                # ask for amount
                context.chat_data["pending_category"] = cat
                await query.message.reply_text("Введите сумму для категории «%s» (например: 1200)." % cat)
                return
            # save expense
            expense = Expense(user_id=user.id, amount=amount, category=cat, created_at=datetime.utcnow())
            db.add(expense)
            db.commit()
            await query.message.reply_text(f"Записано: {amount:.2f} ₽ → {cat}")
            # After saving, check limits
            await check_limits_and_warn(user, context, db)
            return

        if data.startswith("del|"):
            exp_id = int(data.split("|", 1)[1])
            exp = db.query(Expense).filter(Expense.id == exp_id, Expense.user_id == user.id).first()
            if not exp:
                await query.message.reply_text("Трата не найдена или уже удалена.")
                return
            db.delete(exp)
            db.commit()
            await query.message.reply_text("Трата удалена.")
            return

        if data.startswith("recadd|"):
            # Not used in current UI but placeholder
            await query.message.reply_text("Recurring action.")
            return

    finally:
        db.close()

# When user has provided a category previously and now sends amount
async def pending_amount_handler(update: Update, context):
    db = SessionLocal()
    try:
        chat_data = context.chat_data
        pending_cat = chat_data.pop("pending_category", None)
        text = (update.message.text or "").strip()
        cleaned = text.replace(" ", "").replace(",", ".")
        try:
            amount = float(cleaned)
        except Exception:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Не распознал сумму. Повторите, пожалуйста.")
            return
        user = create_user_if_missing(db, update.effective_user)
        expense = Expense(user_id=user.id, amount=amount, category=pending_cat, created_at=datetime.utcnow())
        db.add(expense)
        db.commit()
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Записано: {amount:.2f} ₽ → {pending_cat}")
        # check limits
        await check_limits_and_warn(user, context, db)
    finally:
        db.close()

# Reports & statistics
async def send_statistics(user: User, context):
    db = SessionLocal()
    try:
        if not user.monthly_income or user.monthly_income <= 0:
            await context.bot.send_message(chat_id=user.id, text="Сначала установите месячный доход (введите число).")
            return
        limits = parse_limits(user.limits)
        # compute month-to-date expenses per category and total
        now = datetime.utcnow()
        first_of_month = datetime(now.year, now.month, 1)
        exps = db.query(Expense).filter(Expense.user_id == user.id, Expense.created_at >= first_of_month).all()
        totals = {}
        total_sum = 0.0
        for e in exps:
            totals[e.category] = totals.get(e.category, 0.0) + e.amount
            total_sum += e.amount
        lines = [f"Месячный доход: {user.monthly_income:.2f} ₽\nРасходы за текущий месяц:"]
        for cat, (pct, amt) in limits.items():
            spent = totals.get(cat, 0.0)
            pct_spent = (spent / amt * 100.0) if amt > 0 else 0.0
            lines.append(f"- {cat}: {spent:.2f} ₽ из {amt:.2f} ₽ ({pct}% лимит) — {pct_spent:.1f}%")
        lines.append(f"\nВсего потрачено: {total_sum:.2f} ₽")
        await context.bot.send_message(chat_id=user.id, text="\n".join(lines))
    finally:
        db.close()

async def check_limits_and_warn(user: User, context, db):
    """Check per-category and overall limit and send warnings if exceeded."""
    limits = parse_limits(user.limits)
    now = datetime.utcnow()
    first_of_month = datetime(now.year, now.month, 1)
    # totals for month
    exps = db.query(Expense).filter(Expense.user_id == user.id, Expense.created_at >= first_of_month).all()
    totals = {}
    total_sum = 0.0
    for e in exps:
        totals[e.category] = totals.get(e.category, 0.0) + e.amount
        total_sum += e.amount
    # check each category
    warnings = []
    for cat, (pct, amt) in limits.items():
        spent = totals.get(cat, 0.0)
        if amt > 0 and spent > amt:
            warnings.append(f"⚠️ Лимит по категории «{cat}» превышен: {spent:.2f} ₽ из {amt:.2f} ₽ ({(spent/amt*100):.1f}%).")
    # overall recommended sum = sum of category amounts (should equal income)
    if user.monthly_income and total_sum > user.monthly_income:
        warnings.append(f"⚠️ Общие расходы за месяц ({total_sum:.2f} ₽) превышают доход ({user.monthly_income:.2f} ₽).")
    if warnings:
        for w in warnings:
            try:
                await context.bot.send_message(chat_id=user.id, text=w)
            except Exception:
                logger.exception("Failed to send warning")

# Recurring tasks handling
async def run_recurrings():
    db = SessionLocal()
    try:
        today = datetime.utcnow().date()
        # For all active recurrings, if day matches today's day, create expense
        all_rec = db.query(Recurring).filter(Recurring.active == True).all()
        for r in all_rec:
            # If r.day > last day of month, skip (or add on last day)
            if r.day == today.day:
                exp = Expense(user_id=r.user_id, amount=r.amount, category=r.category, note=f"auto (recurring id {r.id})", created_at=datetime.utcnow())
                db.add(exp)
        db.commit()
    except Exception:
        logger.exception("run_recurrings error")
    finally:
        db.close()

async def daily_reminder():
    """Send reminder to users with notifications enabled to add today's expenses."""
    db = SessionLocal()
    try:
        users = db.query(User).filter(User.notify == True).all()
        for u in users:
            try:
                await bot.send_message(chat_id=u.id, text="Напоминание: не забудьте добавить сегодняшние траты. /help")
            except Exception:
                logger.exception("failed sending daily reminder to %s", u.id)
    finally:
        db.close()

async def weekly_reports():
    """Send a short weekly report (last 7 days) to each user with data."""
    db = SessionLocal()
    try:
        now = datetime.utcnow()
        week_ago = now - timedelta(days=7)
        users = db.query(User).all()
        for u in users:
            exps = db.query(Expense).filter(Expense.user_id == u.id, Expense.created_at >= week_ago).all()
            if not exps:
                continue
            totals = {}
            total_sum = 0.0
            for e in exps:
                totals[e.category] = totals.get(e.category, 0.0) + e.amount
                total_sum += e.amount
            lines = [f"Еженедельный отчёт. Всего: {total_sum:.2f} ₽"]
            for cat, s in totals.items():
                lines.append(f"- {cat}: {s:.2f} ₽")
            try:
                await bot.send_message(chat_id=u.id, text="\n".join(lines))
            except Exception:
                logger.exception("failed to send weekly report to %s", u.id)
    finally:
        db.close()

# --- Webhook endpoint for Telegram ---
class UpdateIn(BaseModel):
    update_id: int

@app.post(f"/webhook/{TOKEN}")
async def telegram_webhook(request: Request):
    """Endpoint to receive updates from Telegram webhook."""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    update = Update.de_json(data, bot)
    # Use application to handle update (python-telegram-bot)
    try:
        await application.process_update(update)
    except Exception:
        logger.exception("Failed processing update")
    return {"ok": True}

# --- Register handlers on application ---
application.add_handler(CommandHandler("start", start_handler))
application.add_handler(CommandHandler("help", help_handler))
application.add_handler(CommandHandler("report", lambda u, c: report_handler(u, c)))
# Message handlers
application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
# If pending category expects amount, handle that (this simplistic approach checks chat_data)
application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), pending_amount_handler))
application.add_handler(CallbackQueryHandler(callback_handler))

# Report handler – supports /report week and /report month
async def report_handler(update: Update, context):
    db = SessionLocal()
    try:
        arg = None
        if update.message and update.message.text:
            parts = update.message.text.split()
            if len(parts) > 1:
                arg = parts[1].lower()
        user = create_user_if_missing(db, update.effective_user)
        now = datetime.utcnow()
        if arg == "week":
            since = now - timedelta(days=7)
            period_name = "7 дней"
        elif arg == "month":
            since = datetime(now.year, now.month, 1)
            period_name = "текущий месяц"
        else:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Использование: /report week или /report month")
            return
        exps = db.query(Expense).filter(Expense.user_id == user.id, Expense.created_at >= since).all()
        if not exps:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Нет трат за {period_name}.")
            return
        totals = {}
        total_sum = 0.0
        for e in exps:
            totals[e.category] = totals.get(e.category, 0.0) + e.amount
            total_sum += e.amount
        lines = [f"Отчёт за {period_name}. Всего: {total_sum:.2f} ₽"]
        for cat, s in totals.items():
            lines.append(f"- {cat}: {s:.2f} ₽")
        await context.bot.send_message(chat_id=update.effective_chat.id, text="\n".join(lines))
    finally:
        db.close()

# --- Startup tasks: set webhook and schedule jobs ---
@app.on_event("startup")
async def on_startup():
    # Set webhook if APP_URL provided
    if APP_URL:
        webhook_url = f"{APP_URL}/webhook/{TOKEN}"
        try:
            await bot.set_webhook(url=webhook_url)
            logger.info("Webhook set to %s", webhook_url)
        except Exception:
            logger.exception("Failed to set webhook")

    # Schedule recurring daily reminder at 09:00 UTC+3 -> that is 06:00 UTC
    # We will schedule daily_reminder at 06:00 UTC and weekly_reports on Monday 06:05 UTC
    try:
        scheduler.add_job(lambda: asyncio.create_task(daily_reminder()), CronTrigger(hour=6, minute=0))
        scheduler.add_job(lambda: asyncio.create_task(weekly_reports()), CronTrigger(day_of_week="mon", hour=6, minute=5))
        # Run recurrings daily at 00:05 UTC
        scheduler.add_job(lambda: asyncio.create_task(run_recurrings()), CronTrigger(hour=0, minute=5))
        logger.info("Scheduled jobs set")
    except Exception:
        logger.exception("Failed to schedule jobs")

@app.on_event("shutdown")
async def on_shutdown():
    try:
        await bot.delete_webhook()
    except Exception:
        pass
    scheduler.shutdown()

# If run locally via uvicorn, start application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)
