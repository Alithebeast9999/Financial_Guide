#!/usr/bin/env python3
"""
Resilient Telegram Finance Assistant (main.py)
- aiohttp web server with / and /webhook
- safe webhook handling via asyncio.Queue + background worker
- graceful shutdown: close bot (aiohttp session), scheduler, storage, DB
- handlers and UI are kept as in user's code
- improved diagnostics: signal logging, /debug and /set_webhook endpoints
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

# ----------------- Logging -----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------- Config ------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 10000))
TZ = pytz.timezone("Europe/Moscow")
# If set to "1" -> force polling mode
FORCE_POLLING = os.environ.get("FORCE_POLLING", "0") == "1"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

# ----------------- Bot / Dispatcher --------
bot = Bot(token=BOT_TOKEN, timeout=30, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ----------------- DB ----------------------
DB_FILE = "bot.db"
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.execute(
    """CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, income REAL DEFAULT 0, notifications BOOLEAN DEFAULT 1)"""
)
cursor.execute(
    """CREATE TABLE IF NOT EXISTS expenses (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, timestamp DATETIME, recurring_id INTEGER DEFAULT NULL)"""
)
cursor.execute(
    """CREATE TABLE IF NOT EXISTS recurring (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, category TEXT, day INTEGER)"""
)
conn.commit()

# ----------------- Categories & states -----
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

# ----------------- Utils & DB helpers -----

db_lock = asyncio.Lock()

async def ensure_user(uid: int):
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

# ----------------- Scheduler ----------------
scheduler = AsyncIOScheduler(timezone=TZ)

async def daily_reminders():
    cursor.execute("SELECT user_id FROM users WHERE notifications = 1")
    for (uid,) in cursor.fetchall():
        try:
            await bot.send_message(uid, "💡 Не забудь добавить траты за сегодня!")
        except Exception as e:
            logger.debug(e)


async def weekly_report():
    cursor.execute("SELECT user_id FROM users")
    for (uid,) in cursor.fetchall():
        try:
            await bot.send_message(uid, "📊 Еженедельный отчёт:\n\n" + format_stats(uid))
        except Exception as e:
            logger.debug(e)


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

scheduler.add_job(daily_reminders, CronTrigger(hour=9, minute=0))
scheduler.add_job(weekly_report, CronTrigger(day_of_week='mon', hour=9, minute=0))
scheduler.add_job(process_recurring, CronTrigger(hour=6, minute=0))

# ----------------- UI helpers ----------------

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
    max_cat_len = max(len(cat) for cat in limits.keys())
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

# ----------------- Handlers -----------------
@dp.message_handler(commands=['start'])
async def start(msg: types.Message):
    uid = msg.from_user.id
    await ensure_user(uid)
    welcome = (
        "<b>Привет! Я — твой финансовый помощник.</b>\n\n"
        "Я помогу тебе отслеживать расходы, планировать бюджет, "
        "настраивать регулярные платежи и вовремя предупреждать о превышениях лимитов.\n\n"
        "Чтобы начать — введите ваш ежемесячный доход (например: <b>(50 000)</b>)\n\n"
        "После ввода дохода я рассчитую рекомендованные лимиты по категориям и покажу подсказки по кнопкам внизу."
    )
    await msg.reply(welcome)


@dp.message_handler(state=IncomeState.income)
async def set_income_handler(msg: types.Message, state: FSMContext):
    try:
        income = float(msg.text.replace(" ", "").replace(",", "."))
    except Exception:
        await msg.reply("❌ Неверный формат дохода. Пример: 50 000")
        return
    set_income(msg.from_user.id, income)
    await state.finish()
    table_html = build_limits_table_html(income)
    buttons_expl = (
        "<b>Кнопки:</b>\n"
        "➕ <b>Добавить трату</b> — добавьте расход вручную: введите сумму и выберите категорию.\n\n"
        "📜 <b>История</b> — просмотр последних трат с категориями, временем и кнопкой удаления.\n\n"
        "📊 <b>Моя статистика</b> — текущие расходы по категориям и сравнение с лимитами.\n\n"
        "ℹ️ <b>Помощь</b> — список доступных команд и быстрых подсказок."
    )
    full_msg = table_html + "\n\n" + buttons_expl
    kb = get_main_keyboard()
    await msg.reply(full_msg, reply_markup=kb)


@dp.message_handler(Text(equals="➕ Добавить трату"))
async def add_expense_cmd(msg: types.Message):
    await msg.reply("💸 Введи сумму траты:")
    await ExpenseState.amount.set()


@dp.message_handler(state=ExpenseState.amount)
async def expense_amount(msg: types.Message, state: FSMContext):
    try:
        amount = float(msg.text.replace(" ", "").replace(",", "."))
        await state.update_data(amount=amount)
        kb = InlineKeyboardMarkup(row_width=2)
        for cat in ALL_CATEGORIES:
            kb.insert(InlineKeyboardButton(cat, callback_data=f"cat_{cat}"))
        await msg.reply("Выбери категорию:", reply_markup=kb)
        await ExpenseState.category.set()
    except:
        await msg.reply("❌ Неверная сумма")


@dp.callback_query_handler(lambda c: c.data.startswith('cat_'), state=ExpenseState.category)
async def expense_category(cb: types.CallbackQuery, state: FSMContext):
    cat = cb.data[4:]
    data = await state.get_data()
    amount = data.get('amount')
    uid = cb.from_user.id
    warnings = check_limits(uid, cat, amount)
    await add_expense(uid, amount, cat)
    await cb.message.edit_text(f"✅ Добавлено: {format_amount(amount)} ₽ — {cat}")
    if warnings:
        await bot.send_message(uid, "\n".join(warnings))
    await state.finish()


@dp.message_handler(Text(equals="📜 История"))
async def history(msg: types.Message):
    exps = get_expenses(msg.from_user.id)
    if not exps:
        await msg.reply("Пока нет трат 💰")
        return
    for e in exps:
        ts = e['timestamp']
        try:
            dt = datetime.fromisoformat(ts).strftime('%d.%m %H:%M')
        except:
            dt = ts
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Удалить", callback_data=f"del_{e['id']}"))
        await msg.reply(f"{dt} | {e['amount']:,.0f} ₽ | {e['category']}", reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith('del_'))
async def delete_expense_cb(cb: types.CallbackQuery):
    eid = int(cb.data[4:])
    delete_expense(eid)
    await cb.answer("Удалено")
    try:
        await cb.message.delete()
    except:
        pass


@dp.message_handler(Text(equals="📊 Моя статистика"))
async def stats(msg: types.Message):
    await msg.reply(format_stats(msg.from_user.id))


@dp.message_handler(Text(equals="ℹ️ Помощь"))
async def help_cmd(msg: types.Message):
    await msg.reply(
        "/report week — отчёт за неделю\n"
        "/report month — отчёт за месяц\n"
        "/add_recurring — добавить регулярный расход\n"
        "/notify — включить/выключить уведомления\n"
    )


@dp.message_handler(commands=['notify'])
async def toggle_notify(msg: types.Message):
    uid = msg.from_user.id
    cursor.execute("SELECT notifications FROM users WHERE user_id = ?", (uid,))
    r = cursor.fetchone()
    current = bool(r['notifications']) if r else True
    new_val = 0 if current else 1
    cursor.execute("UPDATE users SET notifications = ? WHERE user_id = ?", (new_val, uid))
    conn.commit()
    await msg.reply("🔔 Уведомления включены" if new_val else "🔕 Уведомления отключены")


@dp.message_handler(commands=['add_recurring'])
async def add_recurring(msg: types.Message):
    await msg.reply("Введи сумму регулярного расхода:")
    await RecurringState.amount.set()


@dp.message_handler(state=RecurringState.amount)
async def recurring_amount(msg: types.Message, state: FSMContext):
    try:
        amt = float(msg.text.replace(" ", "").replace(",", "."))
        await state.update_data(amount=amt)
        kb = InlineKeyboardMarkup(row_width=2)
        for cat in ALL_CATEGORIES:
            kb.insert(InlineKeyboardButton(cat, callback_data=f"rec_{cat}"))
        await msg.reply("Выбери категорию:", reply_markup=kb)
        await RecurringState.category.set()
    except:
        await msg.reply("❌ Неверная сумма")


@dp.callback_query_handler(lambda c: c.data.startswith('rec_'), state=RecurringState.category)
async def recurring_category(cb: types.CallbackQuery, state: FSMContext):
    cat = cb.data[4:]
    await state.update_data(category=cat)
    await cb.message.edit_text("Укажи день месяца (1–28):")
    await RecurringState.day.set()


@dp.message_handler(state=RecurringState.day)
async def recurring_day(msg: types.Message, state: FSMContext):
    try:
        day = int(msg.text)
        if not (1 <= day <= 28):
            raise ValueError
        data = await state.get_data()
        cursor.execute("INSERT INTO recurring (user_id, amount, category, day) VALUES (?, ?, ?, ?)",
                       (msg.from_user.id, data["amount"], data["category"], day))
        conn.commit()
        await msg.reply(f"🔁 Регулярный расход сохранён: {format_amount(data['amount'])} ₽ — {data['category']} (каждое {day}-е число)")
        await state.finish()
    except:
        await msg.reply("❌ Укажи число от 1 до 28")


@dp.message_handler(commands=['report'])
async def report_cmd(msg: types.Message):
    args = msg.get_args().strip().lower()
    if args not in ('week', 'month'):
        await msg.reply("Используй: /report week или /report month")
        return
    now = datetime.now(TZ)
    start = now - timedelta(days=7) if args == 'week' else now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    cursor.execute("SELECT category, SUM(amount) as total FROM expenses WHERE user_id = ? AND timestamp >= ? GROUP BY category",
                   (msg.from_user.id, start.isoformat()))
    data = cursor.fetchall()
    if not data:
        await msg.reply("Нет данных за выбранный период.")
        return
    text = f"📊 Отчёт за {'неделю' if args == 'week' else 'месяц'}:\n\n"
    for r in data:
        text += f"{r['category']}: {r['total']:,.0f} ₽\n"
    await msg.reply(text)

# ----------------- WEBHOOK queue & worker ----
_updates_queue = None
_worker_task = None

async def webhook_worker():
    logger.info("Webhook worker started")
    try:
        Bot.set_current(bot)
    except Exception:
        pass
    while True:
        try:
            update = await _updates_queue.get()
            try:
                Bot.set_current(bot)
            except Exception:
                pass
            try:
                await dp.process_update(update)
            except Exception:
                logger.exception("Error while processing update (worker)")
            finally:
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
        return web.Response(status=400, text="invalid")
    try:
        _updates_queue.put_nowait(update)
    except asyncio.QueueFull:
        logger.warning("queue full - drop update")
    return web.Response(text="OK")


async def handle_root(request: web.Request):
    return web.Response(text="OK")

# ----------------- keep-alive -----------------
async def keep_alive():
    logger.info("Keep-alive task started")
    try:
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        logger.info("Keep-alive cancelled")
        return

# ----------------- polling runner -------------
async def polling_runner(app):
    backoff = 1
    max_backoff = 60
    while True:
        try:
            Bot.set_current(bot)
        except Exception:
            pass
        try:
            logger.info("Starting dp.start_polling()")
            await dp.start_polling()
            logger.info("dp.start_polling() ended normally")
            break
        except asyncio.CancelledError:
            logger.info("Polling runner cancelled")
            raise
        except Exception:
            logger.exception("Polling crashed, will retry")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)

# ----------------- startup / cleanup ----------
async def on_startup_app(app):
    global _updates_queue, _worker_task
    try:
        Bot.set_current(bot)
    except Exception:
        pass
    try:
        _updates_queue = asyncio.Queue(maxsize=2000)
        _worker_task = asyncio.create_task(webhook_worker())
        app['keep_alive'] = asyncio.create_task(keep_alive())
        scheduler.start()
        logger.info("Scheduler started")

        # choose mode
        if FORCE_POLLING or not WEBHOOK_URL:
            # start resilient polling in background
            app['polling_task'] = asyncio.create_task(polling_runner(app))
            logger.info("Polling mode enabled")
        else:
            # attempt webhook with retry
            webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
            success = False
            for attempt in range(3):
                try:
                    await bot.set_webhook(webhook)
                    logger.info("Webhook set")
                    success = True
                    break
                except Exception:
                    logger.exception("set_webhook failed (attempt %s)", attempt + 1)
                    await asyncio.sleep(1 + attempt * 2)
            if not success:
                logger.warning("set_webhook failed after retries - falling back to polling")
                app['polling_task'] = asyncio.create_task(polling_runner(app))
    except Exception:
        logger.exception("Exception in on_startup_app — continuing startup")


async def on_cleanup_app(app):
    global _updates_queue, _worker_task
    logger.info("on_cleanup_app: starting cleanup")
    # cancel keep-alive
    if app.get('keep_alive'):
        app['keep_alive'].cancel()
        try:
            await app['keep_alive']
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
            pass
        polling_task.cancel()
        try:
            await polling_task
        except Exception:
            pass
    # remove webhook
    try:
        await bot.delete_webhook()
    except Exception:
        pass
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
    except Exception:
        pass
    # close bot session & bot
    try:
        # prefer new API if available
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
                pass
    except Exception:
        logger.exception('Error while closing bot session')
    try:
        await bot.close()
        logger.info('Bot closed (client session closed)')
    except Exception:
        logger.exception('Error while closing bot')
    try:
        conn.close()
        logger.info('DB connection closed')
    except Exception:
        pass
    logger.info('Cleanup complete')


# ----------------- admin endpoints ----------------
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
        'webhook_url_env': WEBHOOK_URL,
    }
    try:
        wh = await bot.get_webhook_info()
        info['telegram_webhook'] = wh.to_python() if wh else None
    except Exception as e:
        info['telegram_webhook_error'] = str(e)
    return web.json_response(info)


# ----------------- create app & run ----------------

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
    # register signal handlers for clearer logs (if supported)
    try:
        loop = asyncio.get_event_loop()
        def _log_signal(sig):
            logger.info(f"Process received signal: {sig}. Initiating graceful shutdown.")
        for s in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(s, _log_signal, s)
            except NotImplementedError:
                # some platforms (Windows) may not support it
                pass
    except Exception:
        pass

    app = create_app()
    logger.info(f"Starting web app on 0.0.0.0:{PORT} (FORCE_POLLING={FORCE_POLLING})")
    web.run_app(app, host='0.0.0.0', port=PORT)
