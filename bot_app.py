#!/usr/bin/env python3
"""
Resilient Telegram Finance Assistant — webhook queue + optional resilient polling.
Исправления:
 - db_lock создаётся на старте (on_startup_app), а не на уровне импорта
 - webhook worker выставляет Dispatcher.set_current(dp) и Bot.set_current(bot) перед обработкой
 - safe shutdown для scheduler (проверка наличия loop)
 - KEEP_BOT_ALIVE не принуждает polling автоматически
"""
import os
import logging
import sqlite3
import asyncio
import signal
from datetime import datetime, timedelta
import pytz
from aiohttp import web

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import Update as TgUpdate
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from aiogram.utils.exceptions import TerminatedByOtherGetUpdates
from aiogram import Bot as AiogramBot
from aiogram.dispatcher.dispatcher import Dispatcher as AiogramDispatcher

# ---------------- Config ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 10000))
TZ = pytz.timezone("Europe/Moscow")

FORCE_POLLING = os.environ.get("FORCE_POLLING", "0") == "1"
KEEP_BOT_ALIVE = os.environ.get("KEEP_BOT_ALIVE", "1") != "0"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

# ---------------- Bot / Dispatcher ---------------
bot = Bot(token=BOT_TOKEN, timeout=30, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ---------------- DB (sqlite) --------------------
DB_FILE = "bot.db"
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.execute("""CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, income REAL DEFAULT 0, notifications BOOLEAN DEFAULT 1)""")
cursor.execute("""CREATE TABLE IF NOT EXISTS expenses (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, timestamp DATETIME, recurring_id INTEGER DEFAULT NULL)""")
cursor.execute("""CREATE TABLE IF NOT EXISTS recurring (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, day INTEGER)""")
conn.commit()

# db_lock is created on startup (not at import) to avoid loop binding issues. <<< FIX
db_lock = None

# ---------------- Categories & states -------------
CATEGORIES = {
    "НАДО": {"Аренда жилья": 0.35, "Продуктовая корзина": 0.15, "Комм. услуги": 0.05, "Связь": 0.03, "Транспорт": 0.05, "Личный уход": 0.02, "Медицина": 0.08},
    "МОГУ": {"Инвестиции": 0.05, "Подушка безопасности": 0.05},
    "ХОЧУ": {"Развлечения": 0.07, "Отдых - путешествия": 0.05, "Покупки": 0.05},
}
ALL_CATEGORIES = [c for g in CATEGORIES.values() for c in g]

class IncomeState(StatesGroup):
    income = State()

class ExpenseState(StatesGroup):
    amount = State()
    category = State()

class RecurringState(StatesGroup):
    amount = State()
    category = State()
    day = State()

# ---------------- Helpers & DB access ------------
async def ensure_user(uid: int):
    global db_lock
    if db_lock is None:
        db_lock = asyncio.Lock()
    async with db_lock:
        cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (uid,))
        conn.commit()

def get_income(uid: int) -> float:
    cursor.execute("SELECT income FROM users WHERE user_id = ?", (uid,))
    r = cursor.fetchone()
    return float(r["income"]) if r and r["income"] is not None else 0.0

def set_income(uid: int, v: float):
    cursor.execute("INSERT OR REPLACE INTO users (user_id, income) VALUES (?, ?)", (uid, v))
    conn.commit()

def format_amount(x):
    try:
        return f"{x:,.0f}".replace(",", " ")
    except Exception:
        return str(x)

def get_limits_from_income(income: float):
    return {cat: income * pct for group in CATEGORIES.values() for cat, pct in group.items()}

async def add_expense(uid, amount, category, ts=None, rec_id=None):
    ts = ts or datetime.now(TZ)
    global db_lock
    if db_lock is None:
        db_lock = asyncio.Lock()
    async with db_lock:
        cursor.execute("INSERT INTO expenses (user_id, amount, category, timestamp, recurring_id) VALUES (?, ?, ?, ?, ?)",
                       (uid, amount, category, ts.isoformat(), rec_id))
        conn.commit()

def get_expenses(uid, limit=10):
    cursor.execute("SELECT id, amount, category, timestamp FROM expenses WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?", (uid, limit))
    return cursor.fetchall()

def delete_expense(eid):
    cursor.execute("DELETE FROM expenses WHERE id = ?", (eid,))
    conn.commit()

def check_limits(uid, category, amount):
    limits = get_limits_from_income(get_income(uid))
    if category not in limits:
        return []
    income = get_income(uid)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    cursor.execute("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ?", (uid, month_start.isoformat(), month_end.isoformat()))
    total_spent = cursor.fetchone()["total"] or 0
    cursor.execute("SELECT SUM(amount) as total FROM expenses WHERE user_id = ? AND category = ? AND timestamp BETWEEN ? AND ?", (uid, category, month_start.isoformat(), month_end.isoformat()))
    cat_spent = cursor.fetchone()["total"] or 0
    msgs = []
    if total_spent + amount > income:
        msgs.append("⚠️ Общий месячный лимит превышен!")
    if cat_spent + amount > limits[category]:
        msgs.append(f"⚠️ Лимит по '{category}' превышен!")
    elif cat_spent + amount > 0.9 * limits[category]:
        msgs.append(f"⚠️ Ты израсходовал более 90% лимита по '{category}'!")
    return msgs

def format_stats(uid: int) -> str:
    income = get_income(uid)
    limits = get_limits_from_income(income)
    month_start = datetime.now(TZ).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = (month_start + timedelta(days=35)).replace(day=1) - timedelta(seconds=1)
    cursor.execute("SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp BETWEEN ? AND ? GROUP BY category",
                   (uid, month_start.isoformat(), month_end.isoformat()))
    rows = cursor.fetchall()
    spent = {r["category"]: r["total"] for r in rows}
    text = f"💰 Ваш доход: {format_amount(income)} ₽\n\n"
    for group, cats in CATEGORIES.items():
        text += f"📂 {group}\n"
        for cat, pct in cats.items():
            lim = limits.get(cat, 0)
            s = spent.get(cat, 0) or 0
            perc = (s / lim * 100) if lim else 0
            text += f"• {cat}: {s:,.0f} ₽ / {lim:,.0f} ₽ ({perc:.0f}%)\n"
        text += "\n"
    return text

# ---------------- Scheduler ----------------
scheduler = AsyncIOScheduler(timezone=TZ)

async def daily_reminders():
    cursor.execute("SELECT user_id FROM users WHERE notifications = 1")
    for (uid,) in cursor.fetchall():
        try:
            await bot.send_message(uid, "💡 Не забудь добавить траты за сегодня!")
        except Exception:
            logger.debug("Failed to send reminder to %s", uid)

async def weekly_report():
    cursor.execute("SELECT user_id FROM users")
    for (uid,) in cursor.fetchall():
        try:
            await bot.send_message(uid, "📊 Еженедельный отчёт:\n\n" + format_stats(uid))
        except Exception:
            logger.debug("Failed to send weekly report to %s", uid)

async def process_recurring():
    today = datetime.now(TZ).day
    cursor.execute("SELECT id, user_id, amount, category FROM recurring WHERE day = ?", (today,))
    for r in cursor.fetchall():
        rec_id, uid, amt, cat = r
        await add_expense(uid, amt, cat, rec_id=rec_id)
        try:
            await bot.send_message(uid, f"🔁 Добавлен регулярный расход: {amt:,.0f} ₽ — {cat}")
        except Exception:
            pass

def _add_scheduler_jobs_once():
    try:
        if not scheduler.get_job("daily_reminders"):
            scheduler.add_job(daily_reminders, CronTrigger(hour=9, minute=0), id="daily_reminders")
        if not scheduler.get_job("weekly_report"):
            scheduler.add_job(weekly_report, CronTrigger(day_of_week='mon', hour=9, minute=0), id="weekly_report")
        if not scheduler.get_job("process_recurring"):
            scheduler.add_job(process_recurring, CronTrigger(hour=6, minute=0), id="process_recurring")
    except Exception:
        logger.exception("Failed to add scheduler jobs")

# ---------------- UI helpers ----------------
def get_main_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("➕ Добавить трату", "📜 История")
    kb.add("📊 Моя статистика", "ℹ️ Помощь")
    return kb

def build_limits_table_html(income: float) -> str:
    limits = get_limits_from_income(income)
    lines = []
    lines.append(f"Доход: {format_amount(income)} ₽")
    lines.append("")
    lines.append("Рекомендуемые лимиты (процент / сумма):")
    lines.append("")
    max_cat_len = max(len(cat) for cat in limits.keys()) if limits else 0
    for group, cats in CATEGORIES.items():
        lines.append(f"{group}:")
        for cat, pct in cats.items():
            sum_rub = limits[cat]
            pct_str = f"{int(pct*100):>2}%"
            cat_name = cat.ljust(max_cat_len)
            sum_str = format_amount(sum_rub).rjust(10)
            lines.append(f"  {cat_name}   {pct_str}   {sum_str} ₽")
        lines.append("")
    pre_block = "<pre>" + "\n".join(lines) + "</pre>"
    return pre_block

# ---------------- Handlers ----------------
@dp.message_handler(commands=['start'])
async def start(msg: types.Message):
    uid = msg.from_user.id
    await ensure_user(uid)
    welcome = (
        "<b>Привет! Я — твой финансовый помощник.</b>\n\n"
        "Я помогу тебе отслеживать расходы, планировать бюджет, "
        "настраивать регулярные платежи и вовремя предупреждать о превышениях лимитов.\n\n"
        "Чтобы начать — введите ваш ежемесячный доход (например: <b>50 000</b>)\n\n"
        "После ввода дохода я рассчитую рекомендованные лимиты по категориям и покажу подсказки по кнопкам внизу."
    )
    kb = get_main_keyboard()
    await IncomeState.income.set()
    await msg.reply(welcome, reply_markup=kb)

# ---------------- WEBHOOK queue & worker ----
_updates_queue = None
_worker_task = None

async def webhook_worker():
    logger.info("Webhook worker started")
    try:
        AiogramBot.set_current(bot)
    except Exception:
        logger.debug("Failed to set current Bot in worker pre-loop")
    while True:
        try:
            update = await _updates_queue.get()
            try:
                # <<< FIX: set Dispatcher and Bot current so FSM (IncomeState) works
                try:
                    AiogramDispatcher.set_current(dp)
                except Exception:
                    try:
                        dp.set_current(dp)
                    except Exception:
                        logger.debug("Failed to set Dispatcher current in worker")
                try:
                    AiogramBot.set_current(bot)
                except Exception:
                    try:
                        bot.set_current(bot)
                    except Exception:
                        logger.debug("Failed to set Bot current in worker")
                await dp.process_update(update)
            except Exception:
                logger.exception("Error while processing update (worker)")
            finally:
                # cleanup context
                try:
                    AiogramDispatcher.set_current(None)
                except Exception:
                    pass
                try:
                    AiogramBot.set_current(None)
                except Exception:
                    pass
                _updates_queue.task_done()
        except asyncio.CancelledError:
            logger.info("Webhook worker cancelled")
            break
        except Exception:
            logger.exception("Unexpected exception in webhook worker; continuing")

async def handle_webhook(request: web.Request):
    try:
        data = await request.json()
        update = TgUpdate.to_object(data)
    except Exception:
        logger.exception("handle_webhook: invalid payload")
        return web.Response(status=400, text="invalid")
    try:
        _updates_queue.put_nowait(update)
    except asyncio.QueueFull:
        logger.warning("queue full - drop update")
    return web.Response(text="OK")

async def handle_root(request: web.Request):
    return web.Response(text="OK")

# ---------------- keep-alive ping ----------------
async def keep_alive_ping():
    while True:
        try:
            await bot.get_me()
        except Exception:
            logger.debug("keep_alive_ping: bot.get_me() failed")
        await asyncio.sleep(300)

# ---------------- polling runner -------------
async def polling_runner(app):
    backoff = 1
    max_backoff = 60
    if app.get('polling_task_running'):
        logger.info("Polling already running; exiting runner")
        return
    app['polling_task_running'] = True
    logger.info("Polling runner started")
    while True:
        try:
            try:
                AiogramBot.set_current(bot)
            except Exception:
                pass
            logger.info("Starting dp.start_polling()")
            await dp.start_polling()
            logger.info("dp.start_polling() ended normally")
            break
        except asyncio.CancelledError:
            logger.info("Polling runner cancelled")
            raise
        except TerminatedByOtherGetUpdates as e:
            logger.warning("Polling terminated by other getUpdates: %s — will backoff and retry", str(e))
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)
            continue
        except Exception:
            logger.exception("Polling crashed, will retry")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)

# ---------------- Startup / Cleanup ----------
_shutdown_initiator = {"signal": None}

def _register_signal_handlers(loop):
    def _on_signal(sig):
        _shutdown_initiator["signal"] = str(sig)
        logger.info(f"Process received signal: {sig}. Marking shutdown_initiator.")
    try:
        loop.add_signal_handler(signal.SIGINT, _on_signal, signal.SIGINT)
        loop.add_signal_handler(signal.SIGTERM, _on_signal, signal.SIGTERM)
        logger.info("Signal handlers registered")
    except NotImplementedError:
        logger.debug("Signal handlers not supported on this platform")

async def on_startup_app(app):
    global _updates_queue, _worker_task, db_lock
    logger.info("on_startup_app: initializing")
    # create db_lock on startup so it binds to running loop <<< FIX
    if db_lock is None:
        db_lock = asyncio.Lock()

    _updates_queue = asyncio.Queue(maxsize=2000)
    _worker_task = asyncio.create_task(webhook_worker())

    if KEEP_BOT_ALIVE:
        app['keep_alive_ping'] = asyncio.create_task(keep_alive_ping())
        logger.info("keep_alive_ping started (KEEP_BOT_ALIVE=True)")

    app['keep_alive'] = asyncio.create_task(asyncio.sleep(3600*24))

    _add_scheduler_jobs_once()
    try:
        scheduler.start()
        logger.info("Scheduler started")
    except Exception:
        logger.exception("Failed to start scheduler")

    # polling only when forced or no webhook configured
    if FORCE_POLLING or not WEBHOOK_URL:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook deleted (pre-polling cleanup)")
        except Exception:
            logger.debug("No webhook to delete or delete failed")
        if not app.get('polling_task'):
            app['polling_task'] = asyncio.create_task(polling_runner(app))
            logger.info("Polling mode enabled")
    else:
        # attempt to set webhook
        webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
        try:
            await bot.set_webhook(webhook)
            logger.info("Webhook set successfully: %s", webhook)
        except Exception:
            logger.exception("set_webhook failed - falling back to polling")
            try:
                await bot.delete_webhook(drop_pending_updates=True)
            except Exception:
                logger.debug("delete_webhook during fallback failed")
            if not app.get('polling_task'):
                app['polling_task'] = asyncio.create_task(polling_runner(app))

async def on_cleanup_app(app):
    global _updates_queue, _worker_task
    logger.info("on_cleanup: starting cleanup (initiator=%s)", _shutdown_initiator.get("signal"))

    # cancel keep-alive
    if app.get('keep_alive'):
        app['keep_alive'].cancel()
        try:
            await app['keep_alive']
        except Exception:
            pass

    if app.get('keep_alive_ping'):
        app['keep_alive_ping'].cancel()
        try:
            await app['keep_alive_ping']
        except Exception:
            pass

    # cancel worker
    if _worker_task:
        _worker_task.cancel()
        try:
            await _worker_task
        except Exception:
            pass

    # drain queue shortly
    if _updates_queue:
        try:
            await asyncio.wait_for(_updates_queue.join(), timeout=2.0)
        except Exception:
            pass
    _updates_queue = None

    # stop polling if active
    polling_task = app.get('polling_task')
    if polling_task:
        try:
            await dp.stop_polling()
        except Exception:
            logger.debug("dp.stop_polling() failed or not available")
        polling_task.cancel()
        try:
            await polling_task
        except Exception:
            pass

    # remove webhook only if we're not keeping the bot alive
    if not KEEP_BOT_ALIVE:
        try:
            await bot.delete_webhook()
            logger.info("Webhook deleted on cleanup")
        except Exception:
            logger.debug("Failed to delete webhook on cleanup")
    else:
        logger.info("KEEP_BOT_ALIVE=True -> skipping webhook deletion")

    # shutdown scheduler safely (check _eventloop) <<< FIX
    try:
        if hasattr(scheduler, "_eventloop") and getattr(scheduler, "_eventloop") is not None:
            try:
                scheduler.shutdown(wait=False)
            except Exception:
                logger.debug("Scheduler shutdown failed")
        else:
            logger.info("Scheduler exists but _eventloop is None (scheduler wasn't started in this process). Skipping shutdown.")
    except Exception:
        logger.exception("Scheduler shutdown check failed")

    # close storage
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
    except Exception:
        logger.debug("Storage close failed")

    # close bot session & bot only if NOT KEEP_BOT_ALIVE
    if not KEEP_BOT_ALIVE:
        try:
            sess = None
            try:
                sess = await bot.get_session()
            except Exception:
                sess = getattr(bot, 'session', None)
            if sess:
                try:
                    await sess.close()
                    logger.info('bot.session closed explicitly')
                except Exception:
                    logger.debug('bot.session.close() failed')
        except Exception:
            logger.exception('Error while closing bot session')
        try:
            await bot.close()
            logger.info('Bot closed (client session closed)')
        except Exception:
            logger.exception('Error while closing bot')
    else:
        logger.info('KEEP_BOT_ALIVE=True -> skipping bot.close()')

    try:
        conn.close()
        logger.info('DB connection closed')
    except Exception:
        pass

    logger.info('Cleanup complete')

# ---------------- Admin endpoints ----------------
async def set_webhook_handler(request: web.Request):
    if not WEBHOOK_URL:
        return web.json_response({"ok": False, "error": "WEBHOOK_URL not configured"}, status=400)
    webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
    try:
        await bot.set_webhook(webhook)
        return web.json_response({"ok": True, "webhook": webhook})
    except Exception as e:
        logger.exception('set_webhook_handler failed')
        return web.json_response({"ok": False, "error": str(e)})

async def debug_handler(request: web.Request):
    info = {
        'queue_size': _updates_queue.qsize() if _updates_queue else None,
        'worker_running': _worker_task is not None and not _worker_task.done(),
        'scheduler_running': scheduler.running,
        'force_polling': FORCE_POLLING,
        'keep_bot_alive': KEEP_BOT_ALIVE,
        'webhook_url_env': WEBHOOK_URL,
    }
    try:
        wh = await bot.get_webhook_info()
        info['telegram_webhook'] = wh.to_python() if wh else None
    except Exception as e:
        info['telegram_webhook_error'] = str(e)
    return web.json_response(info)

# ---------------- Create app & run ----------------
def create_app():
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_post('/webhook', handle_webhook)
    app.router.add_post('/set_webhook', set_webhook_handler)
    app.router.add_get('/debug', debug_handler)
    app.on_startup.append(on_startup_app)
    app.on_cleanup.append(on_cleanup_app)
    return app

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    _register_signal_handlers(loop)
    app = create_app()
    logger.info(f"Starting web app on 0.0.0.0:{PORT} (FORCE_POLLING={FORCE_POLLING}, KEEP_BOT_ALIVE={KEEP_BOT_ALIVE})")
    web.run_app(app, host='0.0.0.0', port=PORT)
