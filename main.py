#!/usr/bin/env python3
"""
Telegram Finance Assistant Bot (Webhook via FastAPI, PTB 20.7+)
- Без Updater
- Асинхронно
- SQLite + SQLAlchemy
- APScheduler
"""

import os
import logging
import asyncio
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Config ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
APP_URL = os.getenv("APP_URL")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data.sqlite")

if not TOKEN:
    raise RuntimeError("❌ TELEGRAM_TOKEN not set in environment")

# --- Database ---
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String)
    monthly_income = Column(Float, default=0.0)
    currency = Column(String, default="₽")
    notify = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    limits = Column(Text, default="")
    expenses = relationship("Expense", back_populates="user", cascade="all, delete-orphan")

class Expense(Base):
    __tablename__ = "expenses"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    amount = Column(Float)
    category = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    user = relationship("User", back_populates="expenses")

Base.metadata.create_all(bind=engine)

# --- Constants ---
CATEGORY_PERCENT = {
    "аренда жилья": 35, "продуктовая корзина": 15, "комм. услуги": 5,
    "связь": 3, "транспорт": 5, "личный уход": 2, "медицина": 8,
    "инвестиции": 5, "подушка безопасности": 5, "развлечения": 7,
    "отдых - путешествия": 5, "шопинг": 5,
}
CATEGORIES = list(CATEGORY_PERCENT.keys())

# --- FastAPI + Scheduler ---
app = FastAPI()
scheduler = AsyncIOScheduler()
scheduler.start()

# --- Helpers ---
def get_user(db, tg_user):
    user = db.query(User).filter(User.id == tg_user.id).first()
    if not user:
        user = User(id=tg_user.id, username=tg_user.username or tg_user.full_name)
        db.add(user)
        db.commit()
    return user

def save_limits(user, db):
    if user.monthly_income <= 0:
        return
    parts = [f"{cat}:{pct}:{round(user.monthly_income * pct / 100, 2)}" for cat, pct in CATEGORY_PERCENT.items()]
    user.limits = ",".join(parts)
    db.commit()

def parse_limits(s):
    out = {}
    for p in s.split(","):
        try:
            c, pct, amt = p.split(":")
            out[c] = (int(pct), float(amt))
        except:
            pass
    return out

def main_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("➕ Добавить трату"), KeyboardButton("📈 Моя статистика")],
            [KeyboardButton("🕘 История"), KeyboardButton("❓ Помощь / Команды")],
        ],
        resize_keyboard=True,
    )

def categories_inline():
    return InlineKeyboardMarkup([[InlineKeyboardButton(c, callback_data=f"cat|{c}")] for c in CATEGORIES])

# --- Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = SessionLocal()
    try:
        user = get_user(db, update.effective_user)
        await update.message.reply_text(
            "Привет! Я помогу отслеживать расходы и лимиты.\nВведите ваш месячный доход (например, 50000).",
            reply_markup=main_keyboard()
        )
    finally:
        db.close()

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start — перезапустить\n"
        "/help — помощь\n"
        "/report week|month — отчёт\n\n"
        "Используйте кнопки или просто вводите число, чтобы добавить трату."
    )

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = SessionLocal()
    try:
        text = (update.message.text or "").strip()
        user = get_user(db, update.effective_user)
        try:
            val = float(text.replace(",", ".").replace(" ", ""))
            if not user.monthly_income:
                user.monthly_income = val
                save_limits(user, db)
                await update.message.reply_text(
                    f"Доход {val:.2f} ₽ сохранён. Лимиты рассчитаны.",
                    reply_markup=main_keyboard()
                )
                return
            context.chat_data["pending_amount"] = val
            await update.message.reply_text(f"Трата {val:.2f} ₽. Выберите категорию:", reply_markup=categories_inline())
            return
        except ValueError:
            pass

        if "помощ" in text.lower() or "команд" in text.lower():
            await help_cmd(update, context)
        elif "статист" in text.lower() or "📈" in text:
            await send_stats(user, update, db)
        elif "истор" in text.lower() or "🕘" in text:
            await send_history(user, update, db)
        else:
            await update.message.reply_text("Не понял. Используйте кнопки или /help.")
    finally:
        db.close()

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    db = SessionLocal()
    try:
        data = query.data
        user = get_user(db, query.from_user)
        if data.startswith("cat|"):
            cat = data.split("|", 1)[1]
            amount = context.chat_data.pop("pending_amount", None)
            if amount is None:
                await query.message.reply_text(f"Введите сумму для категории «{cat}».")
                context.chat_data["pending_category"] = cat
                return
            exp = Expense(user_id=user.id, amount=amount, category=cat)
            db.add(exp)
            db.commit()
            await query.message.reply_text(f"Записано: {amount:.2f} ₽ → {cat}")
    finally:
        db.close()

async def send_stats(user, update, db):
    limits = parse_limits(user.limits)
    now = datetime.utcnow()
    first = datetime(now.year, now.month, 1)
    exps = db.query(Expense).filter(Expense.user_id == user.id, Expense.created_at >= first).all()
    totals = {}
    for e in exps:
        totals[e.category] = totals.get(e.category, 0) + e.amount
    lines = [f"Доход: {user.monthly_income:.2f} ₽\n"]
    for c, (pct, lim) in limits.items():
        spent = totals.get(c, 0)
        lines.append(f"{c}: {spent:.2f}/{lim:.2f} ₽ ({pct}%)")
    await update.message.reply_text("\n".join(lines))

async def send_history(user, update, db):
    exps = db.query(Expense).filter(Expense.user_id == user.id).order_by(Expense.created_at.desc()).limit(10).all()
    if not exps:
        await update.message.reply_text("История пуста.")
        return
    text = "\n".join([f"{e.amount:.2f} ₽ | {e.category} | {e.created_at.strftime('%d.%m')}" for e in exps])
    await update.message.reply_text(text)

# --- FastAPI Webhook ---
class TelegramUpdate(BaseModel):
    update_id: int

@app.post(f"/webhook/{TOKEN}")
async def webhook(request: Request):
    try:
        data = await request.json()
        update = Update.de_json(data, app.bot)
        await app.application.process_update(update)
    except Exception as e:
        logger.exception("Update error: %s", e)
        raise HTTPException(status_code=500)
    return {"ok": True}

# --- Startup / Scheduler ---
@app.on_event("startup")
async def on_startup():
    webhook_url = f"{APP_URL}/webhook/{TOKEN}" if APP_URL else None

    # создаём Application правильно
    app.application = ApplicationBuilder().token(TOKEN).build()
    app.bot = app.application.bot

    app.application.add_handler(CommandHandler("start", start))
    app.application.add_handler(CommandHandler("help", help_cmd))
    app.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    app.application.add_handler(CallbackQueryHandler(callback_handler))

    if webhook_url:
        await app.bot.set_webhook(url=webhook_url)
        logger.info("Webhook set: %s", webhook_url)

    scheduler.add_job(lambda: asyncio.create_task(daily_reminder()), CronTrigger(hour=6, minute=0))
    logger.info("Scheduler started")

@app.on_event("shutdown")
async def on_shutdown():
    await app.bot.delete_webhook()
    scheduler.shutdown()

async def daily_reminder():
    db = SessionLocal()
    try:
        users = db.query(User).filter(User.notify == True).all()
        for u in users:
            await app.bot.send_message(chat_id=u.id, text="Не забудьте добавить траты за сегодня 💰")
    except Exception:
        logger.exception("Reminder error")
    finally:
        db.close()

# --- Local run ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
