import os
import asyncio
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# === Настройки окружения ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("RENDER_EXTERNAL_URL")  # Render автоматически задаёт этот URL
PORT = int(os.getenv("PORT", 8080))

# === Настройка базы данных ===
engine = create_engine("sqlite:///data.sqlite")
Session = sessionmaker(bind=engine)
session = Session()


# === Обработчики ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Бот успешно работает через Webhook 🚀")


# === Основная функция ===
async def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    # Установка webhook
    await app.bot.set_webhook(f"{WEBHOOK_URL}/webhook")

    print("✅ Бот запущен и webhook установлен:", f"{WEBHOOK_URL}/webhook")

    # Запуск сервера (без asyncio.run)
    await app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_url=f"{WEBHOOK_URL}/webhook",
        drop_pending_updates=True,
    )


# === Корректный запуск ===
if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except RuntimeError:
        # Если Render уже создал event loop
        asyncio.run(main())
