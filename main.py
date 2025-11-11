import logging
import asyncio
import os
from aiogram import Bot, Dispatcher, types
from aiogram.utils.executor import start_webhook
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.types import Message
from dotenv import load_dotenv
import aiosqlite
from datetime import datetime
from aiohttp import web

# Загружаем токен из .env
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Настройки webhook
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "https://financial-guide.onrender.com")
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{WEBHOOK_HOST.rstrip('/')}{WEBHOOK_PATH}"

# Настройки хоста Render — берём порт из env (Render требует привязки к PORT)
WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", 10000))

# Инициализация aiogram
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не установлен в окружении")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_PATH = "bot_data.db"


# === БАЗА ДАННЫХ ===
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                joined_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT,
                created_at TEXT
            )
        """)
        await db.commit()
    logging.info("База данных инициализирована.")


async def add_user(user: types.User):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR IGNORE INTO users (id, username, first_name, last_name, joined_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            user.id,
            getattr(user, "username", None),
            getattr(user, "first_name", None),
            getattr(user, "last_name", None),
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        ))
        await db.commit()


async def log_action(user_id: int, action: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO logs (user_id, action, created_at)
            VALUES (?, ?, ?)
        """, (
            user_id,
            action,
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        ))
        await db.commit()


# === MIDDLEWARE для логирования ===
class LoggingMiddleware(BaseMiddleware):
    async def on_pre_process_message(self, message: Message, data: dict):
        # защита: message.from_user может быть None — но в обычных сообщениях оно есть
        if message.from_user:
            await add_user(message.from_user)
            await log_action(message.from_user.id, message.text or "command")
            logging.info(f"[{message.from_user.id}] {message.text}")


dp.middleware.setup(LoggingMiddleware())


# === ОБРАБОТЧИКИ ===
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await add_user(message.from_user)
    await message.answer(
        f"👋 Привет, {message.from_user.first_name}!\n"
        "Я — Financial Guide Bot.\n\n"
        "Я помогу тебе лучше понять финансы, инвестиции и экономику.\n"
        "Используй /help чтобы узнать больше."
    )


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    await message.answer(
        "📘 Доступные команды:\n"
        "/start — начать заново\n"
        "/help — справка\n"
        "/stats — статистика\n"
        "/feedback — оставить отзыв"
    )


@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cur:
            user_count = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM logs") as cur:
            actions_count = (await cur.fetchone())[0]

    await message.answer(
        f"📊 Статистика:\n"
        f"Пользователей: {user_count}\n"
        f"Действий за всё время: {actions_count}"
    )


@dp.message_handler(commands=["feedback"])
async def cmd_feedback(message: types.Message):
    await message.answer("✉️ Отправь свой отзыв прямо здесь, и я передам его администратору.")


@dp.message_handler(lambda m: not (m.text or "").startswith("/"))
async def echo_text(message: types.Message):
    await message.answer("💡 Спасибо за сообщение! Я передам его администратору.")


# === ХУКИ WEBHOOK ===
async def on_startup(dp_):
    # dp_ параметр оставляем для совместимости с aiogram
    logging.info("on_startup: init_db")
    await init_db()
    # ставим webhook
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logging.info(f"Webhook установлен: {WEBHOOK_URL}")
    except Exception as e:
        logging.exception("Не удалось установить webhook на старте: %s", e)


async def on_shutdown(dp_):
    logging.warning("on_shutdown: удаляем webhook и закрываем бот...")
    try:
        await bot.delete_webhook()
        logging.info("Webhook удалён.")
    except Exception:
        logging.exception("Ошибка при удалении webhook")
    try:
        await bot.close()
        logging.info("Bot closed")
    except Exception:
        logging.exception("Ошибка при закрытии бота")


# === AIOHTTP web_app для healthchecks (FIX: root должен возвращать 200) ===
def make_web_app():
    app = web.Application()

    async def root_handler(request):
        return web.Response(text="OK", status=200)

    async def health_handler(request):
        return web.json_response({"ok": True, "uptime": True})

    # Регистрируем только GET — aiohttp сам обслужит HEAD для GET.
    app.router.add_get("/", root_handler)
    app.router.add_get("/health", health_handler)

    return app


# === ЗАПУСК ===
if __name__ == "__main__":
    # явная установка event loop — чтобы избежать RuntimeError в aiogram.start_webhook
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # инициализируем базу (синхронно в этом потоке)
    loop.run_until_complete(init_db())

    # создаём aiohttp приложение и передаём его в start_webhook,
    # чтобы корневой route ("/") отвечал 200 и проверки проходили
    web_app = make_web_app()

    logging.info("Запускаю start_webhook (web_app + aiogram)...")
    start_webhook(
        dispatcher=dp,
        webhook_path=WEBHOOK_PATH,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host=WEBAPP_HOST,
        port=WEBAPP_PORT,
        web_app=web_app,   # <- ключевое изменение: теперь '/' возвращает 200
    )
