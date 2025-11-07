import logging
import os
import asyncio
import json
import aiohttp
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.types import ContentType
from aiogram.utils.executor import start_webhook
from aiogram.dispatcher.filters import Text
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from aiohttp import web

# === Настройка логов ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Загрузка переменных окружения ===
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_HOST = os.getenv("RENDER_EXTERNAL_URL", "https://financial-guide.onrender.com")
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

# === Инициализация бота и диспетчера ===
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# === Подключение к БД ===
async def create_db_pool():
    return await asyncpg.create_pool(DATABASE_URL)

db_pool = None

# === Планировщик задач ===
scheduler = AsyncIOScheduler()

async def scheduled_reminder():
    logger.info("📅 Scheduled task executed.")

scheduler.add_job(scheduled_reminder, "interval", hours=24)

# === Keep Alive ===
async def keep_alive():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(WEBHOOK_HOST) as resp:
                    logger.info(f"Keep-alive ping: {resp.status}")
        except Exception as e:
            logger.warning(f"Keep-alive failed: {e}")
        await asyncio.sleep(300)

# === Основные хендлеры ===
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.answer("👋 Привет! Я Финансовый помощник. Готов к работе!")

@dp.message_handler(Text(equals="➕ Добавить трату"))
async def add_expense(message: types.Message):
    await message.answer("💸 Введи сумму и категорию траты, например: 150 продукты")

@dp.message_handler(content_types=ContentType.TEXT)
async def handle_text(message: types.Message):
    await message.answer(f"Вы написали: {message.text}")

# === Webhook обработчик ===
async def on_startup_app(app: web.Application):
    global db_pool
    logger.info("🚀 on_startup_app: инициализация...")

    db_pool = await create_db_pool()
    logger.info("✅ База данных подключена")

    await bot.set_webhook(WEBHOOK_URL)
    logger.info(f"✅ Webhook установлен: {WEBHOOK_URL}")

    scheduler.start()
    logger.info("✅ Планировщик запущен")

    asyncio.create_task(keep_alive())

async def on_cleanup_app(app: web.Application):
    logger.info("🧹 on_cleanup_app: очистка ресурсов...")

    scheduler.shutdown()
    logger.info("⏹ Планировщик остановлен")

    await bot.delete_webhook()
    logger.info("❌ Webhook удалён")

    if db_pool:
        await db_pool.close()
        logger.info("📦 Подключение к БД закрыто")

    try:
        await bot.close()
        logger.info("🤖 Сессия бота закрыта")
    except Exception as e:
        logger.error(f"Ошибка при закрытии бота: {e}")

# === Инициализация aiohttp ===
async def handle_webhook(request):
    try:
        update = types.Update(**await request.json())
        await dp.process_update(update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Ошибка в webhook: {e}")
        return web.Response(status=500)

app = web.Application()
app.router.add_get("/", lambda _: web.Response(text="Bot is alive!"))
app.router.add_post(WEBHOOK_PATH, handle_webhook)

app.on_startup.append(on_startup_app)
app.on_cleanup.append(on_cleanup_app)

# === Запуск ===
if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    web.run_app(app, host="0.0.0.0", port=port)
