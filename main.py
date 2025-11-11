#!/usr/bin/env python3
import logging
import asyncio
import os
from aiogram import Bot, Dispatcher, types
# removed start_webhook
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.types import Message, Update as TgUpdate
from dotenv import load_dotenv
import aiosqlite
from datetime import datetime
from aiohttp import web

# load env
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

# webhook settings
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "https://financial-guide.onrender.com")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_URL = f"{WEBHOOK_HOST.rstrip('/')}{WEBHOOK_PATH}"

# host / port
WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", 10000))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set in env")

# init bot/dispatcher
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_PATH = "bot_data.db"

# === DB helpers ===
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

# === Middleware ===
class LoggingMiddleware(BaseMiddleware):
    async def on_pre_process_message(self, message: Message, data: dict):
        # non-blocking best-effort (we await to ensure DB writes)
        await add_user(message.from_user)
        await log_action(message.from_user.id, message.text or "command")
        logging.info(f"[{message.from_user.id}] {message.text}")

dp.middleware.setup(LoggingMiddleware())

# === Handlers ===
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

# ========== Webhook queue + worker ==========
_updates_queue: asyncio.Queue = None
_worker_task: asyncio.Task = None

async def webhook_worker():
    logging.info("Webhook worker started")
    # Set Bot current for aiogram context
    try:
        Bot.set_current(bot)
    except Exception:
        logging.debug("Bot.set_current failed (ignored)")
    while True:
        try:
            update = await _updates_queue.get()
            try:
                # ensure bot is current in this thread/context
                try:
                    Bot.set_current(bot)
                except Exception:
                    pass
                await dp.process_update(update)
            except Exception:
                logging.exception("Error while processing update in worker")
            finally:
                _updates_queue.task_done()
        except asyncio.CancelledError:
            logging.info("Webhook worker cancelled")
            break
        except Exception:
            logging.exception("Unexpected exception in webhook worker; continuing")

# aiohttp handler
async def handle_webhook(request: web.Request):
    try:
        data = await request.json()
    except Exception:
        logging.exception("Invalid JSON in webhook")
        return web.Response(status=400, text="invalid json")
    try:
        update = TgUpdate.to_object(data)
    except Exception:
        logging.exception("Could not parse update to aiogram Update")
        return web.Response(status=400, text="invalid update")
    try:
        _updates_queue.put_nowait(update)
    except asyncio.QueueFull:
        logging.warning("Updates queue is full — dropping update")
    return web.Response(text="OK")

async def handle_root(request: web.Request):
    return web.Response(text="OK")

# startup / cleanup for aiohttp app
async def on_startup(app: web.Application):
    global _updates_queue, _worker_task
    logging.info("on_startup: init db and set webhook")
    # init db
    await init_db()
    # queue & worker
    _updates_queue = asyncio.Queue(maxsize=1000)
    _worker_task = asyncio.create_task(webhook_worker())
    # set webhook
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logging.info(f"Webhook установлен: {WEBHOOK_URL}")
    except Exception:
        logging.exception("Failed to set webhook on startup")

async def on_cleanup(app: web.Application):
    global _updates_queue, _worker_task
    logging.info("on_cleanup: graceful shutdown")
    # cancel worker
    if _worker_task:
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            logging.info("Worker cancelled cleanly")
        except Exception:
            logging.exception("Exception while waiting worker")
    # drain queue (best-effort)
    if _updates_queue:
        try:
            await asyncio.wait_for(_updates_queue.join(), timeout=2.0)
        except Exception:
            pass
    _updates_queue = None
    # delete webhook
    try:
        await bot.delete_webhook()
        logging.info("Webhook удалён")
    except Exception:
        logging.exception("Failed to delete webhook on cleanup")
    # close bot session + bot
    try:
        sess = getattr(bot, "session", None)
        if sess:
            try:
                await sess.close()
                logging.info("bot.session closed explicitly")
            except Exception:
                logging.exception("Failed to close bot.session")
    except Exception:
        logging.exception("While checking bot.session")
    try:
        await bot.close()
        logging.info("Bot closed")
    except Exception:
        logging.exception("Error closing bot")
    logging.info("Cleanup finished")

def create_app():
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

# ========== Entrypoint ==========
if __name__ == "__main__":
    app = create_app()
    logging.info(f"Starting aiohttp web server on {WEBAPP_HOST}:{WEBAPP_PORT} (WEBHOOK_URL={WEBHOOK_URL})")
    web.run_app(app, host=WEBAPP_HOST, port=WEBAPP_PORT)
