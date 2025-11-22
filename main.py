# main.py — hardened webhook-only runner with self-ping + worker supervisor
import os
import logging
import asyncio
import signal
from typing import Optional
from aiohttp import web, ClientSession, ClientTimeout
import aiohttp

import bot_app  # imports bot, dp, scheduler, handlers registered on import
from aiogram.types import Update

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # Render normally provides this
PORT = int(os.environ.get("PORT", 10000))

# Keep bot alive outgoing (optional) — retained from previous working code
KEEP_BOT_ALIVE = os.environ.get("KEEP_BOT_ALIVE", "1") != "0"
# Self-ping to ensure incoming HTTP traffic and avoid platform sleep
SELF_PING_INTERVAL = int(os.environ.get("SELF_PING_INTERVAL", "60"))  # seconds

# Shared references from bot_app
bot = bot_app.bot
dp = bot_app.dp
scheduler = bot_app.scheduler

# Runtime artifacts
_updates_queue: Optional[asyncio.Queue] = None
_worker_task: Optional[asyncio.Task] = None
_self_ping_task: Optional[asyncio.Task] = None
_self_ping_session: Optional[ClientSession] = None
_keep_alive_ping_task: Optional[asyncio.Task] = None
_supervisor_task: Optional[asyncio.Task] = None

_shutdown_initiator = {"signal": None}


def _register_signal_handlers(loop: asyncio.AbstractEventLoop):
    def _on_signal(sig):
        _shutdown_initiator["signal"] = str(sig)
        logger.info("Process received signal: %s. Marking shutdown_initiator.", sig)
    try:
        loop.add_signal_handler(signal.SIGINT, _on_signal, signal.SIGINT)
        loop.add_signal_handler(signal.SIGTERM, _on_signal, signal.SIGTERM)
        logger.info("Signal handlers registered")
    except NotImplementedError:
        logger.debug("Signal handlers not supported on this platform")


def _set_loop_exception_handler(loop: asyncio.AbstractEventLoop):
    def _handle(loop, context):
        # context may contain 'exception' and 'message'
        msg = context.get("message")
        exc = context.get("exception")
        if exc:
            logger.exception("Unhandled exception in event loop: %s", exc)
        else:
            logger.error("Event loop context: %s", msg)
        # Do not stop the loop here; just log — supervisor tasks will try to recover
    loop.set_exception_handler(_handle)


async def webhook_worker():
    """Background worker that processes Update objects from the queue."""
    logger.info("Webhook worker started")
    # set current Bot/Dispatcher best-effort at worker startup for aiogram internals
    try:
        try:
            bot_app.Bot.set_current(bot)
            logger.debug("Bot.set_current(bot) OK in worker startup")
        except Exception:
            logger.debug("Bot.set_current failed in worker startup", exc_info=True)
        try:
            bot_app.Dispatcher.set_current(dp)
            logger.debug("Dispatcher.set_current(dp) OK in worker startup")
        except Exception:
            logger.debug("Dispatcher.set_current failed in worker startup", exc_info=True)
    except Exception:
        logger.debug("Unexpected exception during worker startup context set", exc_info=True)

    while True:
        try:
            update: Update = await _updates_queue.get()
            try:
                # ensure context each iteration (best-effort)
                try:
                    bot_app.Bot.set_current(bot)
                except Exception:
                    pass
                try:
                    bot_app.Dispatcher.set_current(dp)
                except Exception:
                    pass

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


async def _supervisor():
    """
    Monitor _worker_task and restart it if it dies unexpectedly.
    Also monitors self-ping and will log and attempt to restart it.
    """
    logger.info("Supervisor started")
    try:
        while True:
            try:
                # monitor worker
                global _worker_task
                if _worker_task is None or _worker_task.done():
                    # If it's done, log result and restart
                    if _worker_task is not None:
                        try:
                            res = _worker_task.result()
                            logger.warning("Worker finished with result: %r", res)
                        except Exception as e:
                            logger.exception("Worker finished with exception: %s", e)
                    logger.info("Restarting webhook worker")
                    _worker_task = asyncio.create_task(webhook_worker())
                # monitor self-ping task
                if WEBHOOK_URL:
                    if _self_ping_task is None or _self_ping_task.done():
                        logger.info("Restarting self_ping task")
                        # _start_self_ping will create session/task; call directly
                        await _start_self_ping_task()
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Supervisor loop error; continuing")
                await asyncio.sleep(5)
    finally:
        logger.info("Supervisor stopped")


async def handle_webhook(request: web.Request):
    """
    Webhook endpoint: receives Telegram POST, builds aiogram.types.Update,
    enqueues for background processing and returns HTTP 200 quickly.
    """
    try:
        data = await request.json()
        update = Update(**data)
    except Exception:
        logger.exception("handle_webhook: invalid payload")
        return web.Response(status=400, text="invalid")
    try:
        _updates_queue.put_nowait(update)
    except asyncio.QueueFull:
        logger.warning("updates queue full - dropping incoming update")
    return web.Response(text="OK")


async def handle_root(request: web.Request):
    return web.Response(text="OK")


async def health_handler(request: web.Request):
    return web.Response(text="OK")


async def set_webhook_handler(request: web.Request):
    if not WEBHOOK_URL:
        return web.json_response({"ok": False, "error": "WEBHOOK_URL not configured"}, status=400)
    webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
    try:
        await bot.set_webhook(webhook)
        logger.info("Webhook set via admin endpoint: %s", webhook)
        return web.json_response({"ok": True, "webhook": webhook})
    except Exception as e:
        logger.exception("set_webhook_handler failed")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def debug_handler(request: web.Request):
    info = {
        'queue_size': _updates_queue.qsize() if _updates_queue else None,
        'worker_running': _worker_task is not None and not _worker_task.done(),
        'scheduler_running': getattr(scheduler, "running", None),
        'webhook_url_env': WEBHOOK_URL,
        'keep_bot_alive': KEEP_BOT_ALIVE,
        'self_ping_running': _self_ping_task is not None and not _self_ping_task.done(),
    }
    try:
        wh = await bot.get_webhook_info()
        info['telegram_webhook'] = wh.to_python() if wh else None
    except Exception as e:
        info['telegram_webhook_error'] = str(e)
    return web.json_response(info)


async def keep_alive_ping():
    logger.info("keep_alive_ping started")
    try:
        while True:
            try:
                await bot.get_me()
            except Exception:
                logger.debug("keep_alive_ping: bot.get_me() failed", exc_info=True)
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        logger.info("keep_alive_ping cancelled")
        raise


async def _self_ping_loop(public_root: str):
    """
    Periodically perform GET to public_root (root path) to create incoming traffic.
    Session is created externally and passed via closure via _self_ping_session.
    """
    global _self_ping_session
    logger.info("self_ping loop started -> %s (interval=%ss)", public_root, SELF_PING_INTERVAL)
    try:
        if _self_ping_session is None:
            _self_ping_session = aiohttp.ClientSession(timeout=ClientTimeout(total=10))
        while True:
            try:
                # Use GET to the public URL root (should hit app's incoming request log)
                async with _self_ping_session.get(public_root, allow_redirects=True) as resp:
                    logger.debug("self_ping %s -> %s", public_root, resp.status)
            except Exception as e:
                logger.debug("self_ping request failed: %s", e)
            await asyncio.sleep(SELF_PING_INTERVAL)
    except asyncio.CancelledError:
        logger.info("self_ping loop cancelled")
        raise


async def _start_self_ping_task():
    """
    Start the self-ping task (ensures session created and task scheduled).
    """
    global _self_ping_task, _self_ping_session
    if not WEBHOOK_URL:
        logger.debug("WEBHOOK_URL not set -> skipping self-ping")
        return
    # find base public root (strip trailing paths)
    public_root = WEBHOOK_URL.rstrip("/")
    # create session if not exists
    if _self_ping_session is None:
        _self_ping_session = aiohttp.ClientSession(timeout=ClientTimeout(total=10))
    if _self_ping_task is None or _self_ping_task.done():
        _self_ping_task = asyncio.create_task(_self_ping_loop(public_root))
        logger.info("self_ping task created")


async def _stop_self_ping_task():
    global _self_ping_task, _self_ping_session
    if _self_ping_task:
        _self_ping_task.cancel()
        try:
            await _self_ping_task
        except Exception:
            pass
        _self_ping_task = None
    if _self_ping_session:
        try:
            await _self_ping_session.close()
        except Exception:
            pass
        _self_ping_session = None


async def on_startup_app(app: web.Application):
    global _updates_queue, _worker_task, _supervisor_task, _keep_alive_ping_task
    logger.info("on_startup_app: initializing")

    # configure loop-level exception handler
    loop = asyncio.get_event_loop()
    _register_signal_handlers(loop)
    _set_loop_exception_handler(loop)

    # initialize bot_app internals (DB, scheduler jobs, etc.)
    await bot_app.init_app_for_runtime(app)

    # queue + worker
    _updates_queue = asyncio.Queue(maxsize=2000)
    app['updates_queue'] = _updates_queue
    _worker_task = asyncio.create_task(webhook_worker())

    # start supervisor
    _supervisor_task = asyncio.create_task(_supervisor())

    # self-ping to ensure incoming HTTP traffic
    if WEBHOOK_URL:
        await _start_self_ping_task()

    # keep_alive outgoing ping (bot.get_me)
    if KEEP_BOT_ALIVE:
        _keep_alive_ping_task = asyncio.create_task(keep_alive_ping())

    # small dummy to keep loop busy (optional)
    app['keep_alive'] = asyncio.create_task(asyncio.sleep(3600 * 24))
    logger.debug("keep_alive dummy task created")

    # set webhook if configured
    if WEBHOOK_URL:
        webhook = WEBHOOK_URL.rstrip("/") + "/webhook"
        success = False
        for attempt in range(3):
            try:
                await bot.set_webhook(webhook)
                logger.info("Webhook set successfully: %s", webhook)
                success = True
                break
            except Exception:
                logger.exception("set_webhook failed (attempt %s)", attempt + 1)
                await asyncio.sleep(1 + attempt * 2)
        if not success:
            logger.warning("set_webhook failed after retries - webhook not set. Check WEBHOOK_URL and network.")
    else:
        logger.warning("WEBHOOK_URL not configured; webhook not set. App will still run but won't receive Telegram updates.")


async def on_cleanup_app(app: web.Application):
    """
    Graceful shutdown: cancel tasks, delete webhook, stop scheduler, close db and client sessions.
    """
    global _updates_queue, _worker_task, _self_ping_task, _self_ping_session, _supervisor_task, _keep_alive_ping_task
    logger.info("on_cleanup: starting cleanup (initiator=%s)", _shutdown_initiator.get("signal"))

    # cancel keep_alive dummy
    if app.get('keep_alive'):
        app['keep_alive'].cancel()
        try:
            await app['keep_alive']
        except Exception:
            pass

    # stop supervisor
    if _supervisor_task:
        _supervisor_task.cancel()
        try:
            await _supervisor_task
        except Exception:
            pass
        _supervisor_task = None

    # cancel self-ping
    await _stop_self_ping_task()

    # cancel keep_alive_ping
    if _keep_alive_ping_task:
        _keep_alive_ping_task.cancel()
        try:
            await _keep_alive_ping_task
        except Exception:
            pass
        _keep_alive_ping_task = None

    # cancel worker
    if _worker_task:
        _worker_task.cancel()
        try:
            await _worker_task
        except Exception:
            pass
        _worker_task = None

    # drain queue shortly
    if _updates_queue:
        try:
            await asyncio.wait_for(_updates_queue.join(), timeout=2.0)
        except Exception:
            pass
    _updates_queue = None

    # delete webhook (best-effort)
    if WEBHOOK_URL:
        try:
            await bot.delete_webhook()
            logger.info("Webhook deleted on cleanup")
        except Exception:
            logger.debug("Failed to delete webhook on cleanup", exc_info=True)

    # shutdown scheduler
    try:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shutdown")
    except Exception:
        logger.debug("Scheduler shutdown failed", exc_info=True)

    # close storage
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
        logger.info("Storage closed")
    except Exception:
        logger.debug("Storage close failed", exc_info=True)

    # close bot session & bot
    try:
        sess = None
        try:
            sess = await bot.get_session()
        except Exception:
            sess = getattr(bot, 'session', None)
        if sess:
            try:
                await sess.close()
                logger.info("bot.session closed explicitly")
            except Exception:
                logger.exception("Error while closing bot session")
    except Exception:
        logger.exception("Failed while closing bot session")
    try:
        await bot.close()
        logger.info("Bot closed (client session closed)")
    except Exception:
        logger.exception("Error while closing bot")

    # close aiosqlite DB (bot_app.close_db handles guard)
    try:
        await bot_app.close_db()
    except Exception:
        logger.exception("Error while closing DB via bot_app.close_db()")

    logger.info("Cleanup complete")


def create_app():
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_post("/webhook", handle_webhook)
    app.router.add_get("/healthz", health_handler)
    app.router.add_post("/set_webhook", set_webhook_handler)
    app.router.add_get("/debug", debug_handler)
    app.on_startup.append(on_startup_app)
    app.on_cleanup.append(on_cleanup_app)
    return app


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    # we won't let aiohttp auto-install signal handlers (use our own)
    app = create_app()
    logger.info("Starting webhook-only web app on 0.0.0.0:%s (WEBHOOK_URL=%s)", PORT, bool(WEBHOOK_URL))
    web.run_app(app, host="0.0.0.0", port=PORT, handle_signals=False)
