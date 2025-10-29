import asyncio
from telegram.ext import Application, CommandHandler

TOKEN = "YOUR_BOT_TOKEN"
WEBHOOK_URL = "https://your-app-name.onrender.com"  # замени на актуальный Render URL

async def start(update, context):
    await update.message.reply_text("Бот запущен!")

async def main():
    app = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", start))

    print("✅ Бот успешно запущен. Webhook активен.")

    await app.run_webhook(
        listen="0.0.0.0",
        port=10000,
        url_path=TOKEN,
        webhook_url=f"{WEBHOOK_URL}/{TOKEN}",
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    try:
        # Проверяем, есть ли уже запущенный event loop
        asyncio.get_running_loop()
    except RuntimeError:
        # Если нет — создаём новый
        asyncio.run(main())
    else:
        # Если есть (Render, Jupyter и т.п.), просто запускаем таск
        asyncio.create_task(main())
