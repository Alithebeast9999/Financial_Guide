# main.py
"""
Entrypoint for Financial_Guide — robust, with auto-restart and heartbeat.
(Исправлённая версия: гарантированно выставляет Dispatcher/ Bot current в webhook-пути,
 безопасно останавливает scheduler, и не полагается на on_cleanup как «флаг» для авто-рестарта.)
"""
import os
import sys
import asyncio
import signal
import logging
import traceback
import json
import datetime as _dt
from typing import Optional, Tuple, Any

from aiohttp import web, ClientSession, ClientTimeout
from aiogram.utils.exceptions import TerminatedByOtherGetUpdates
from aiogram import types as aiogram_types

import bot_app

# --------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

# --------------------
PORT = int(os.environ.get("PORT", "10000"))
RUN_POLLING = os.environ.get("RUN_POLLING", "0").lower() in ("1", "true", "yes")
CLIENT_SESSION_TIMEOUT_S = float(os.environ.get("CLIENT_SESSION_TIMEOUT_S", "60"))
POLLING_SHUTDOWN_TIMEOUT = float(os.environ.get("POLLING_SHUTDOWN_TIMEOUT", "6.0"))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # e.g. "https://yourdomain.com"
WEBHOOK_PREFIX = os.environ.get("WEBHOOK_PREFIX", "/webhook").rstrip("/")
WEBHOOK_AUTO_DELETE = os.environ.get("WEBHOOK_AUTO_DELETE", "0") in ("1", "true", "yes")

AUTO_RESTART = os.environ.get("AUTO_RESTART", "1") in ("1", "true", "yes")
AUTO_RESTART_MAX = int(os.environ.get("AUTO_RESTART_MAX", "20"))
AUTO_RESTART_DELAY_S = float(os.environ.get("AUTO_RESTART_DELAY_S", "2.0"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "30.0"))

# --------------------
# Set to True only when a real external OS signal requested shutdown (SIGINT/SIGTERM).
SHUTDOWN_REQUESTED = False

# --------------------
app = web.Application()
app["boot_time"] = _dt.datetime.utcnow().isoformat()
app["polling_task"] = None
app["client_session"] = None
app["bot"] = None
app["dp"] = None
app["shutting_down"] = False
app["webhook_set_by_main"] = False
app["webhook_full_url"] = None
app["heartbeat_task"] = None
app["cleanup_skipped_due_no_signal"] = False

# --------------------
def safe_getattr(obj: Any, name: str, default: Any = None):
    try:
        return getattr(obj, name, default)
    except Exception:
        return default

async def maybe_await(obj):
    if asyncio.iscoroutine(obj):
        return await obj
    return obj

def _log_exception(exc: Optional[BaseException] = None, msg: str = "Exception"):
    if exc is None:
        logger.exception(msg)
    else:
        logger.error("%s: %s", msg, exc)
        logger.debug("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))

# --------------------
def create_client_session() -> ClientSession:
    timeout = ClientTimeout(total=CLIENT_SESSION_TIMEOUT_S)
    session = ClientSession(timeout=timeout)
    logger.info("Created shared aiohttp ClientSession (timeout=%ss)", CLIENT_SESSION_TIMEOUT_S)
    return session

def _has_attr(module, name: str) -> bool:
    try:
        return hasattr(module, name)
    except Exception:
        return False

def _get_bot_and_dp_from_module(module) -> Tuple[Optional[Any], Optional[Any]]:
    bot = safe_getattr(module, "bot", None)
    dp = safe_getattr(module, "dp", None)
    if bot and dp:
        logger.info("Found bot and dp at module-level (fallback).")
    else:
        logger.info("Module-level bot/dp not found (fallback may be incomplete). bot=%s dp=%s", bool(bot), bool(dp))
    return bot, dp

async def _init_bot_with_session_or_fallback(session: Optional[ClientSession]):
    bot = None
    dp = None
    if _has_attr(bot_app, "init_bot"):
        try:
            logger.info("Calling bot_app.init_bot(session) ...")
            maybe = bot_app.init_bot(session)
            res = await maybe_await(maybe)
            if isinstance(res, tuple) and len(res) >= 2:
                bot, dp = res[0], res[1]
                logger.info("bot_app.init_bot returned (bot, dp).")
            elif res is None:
                bot, dp = _get_bot_and_dp_from_module(bot_app)
            else:
                bot = res
                dp = getattr(bot_app, "dp", None)
        except Exception as ex:
            _log_exception(ex, "bot_app.init_bot(session) failed (falling back to module level)")
            bot, dp = _get_bot_and_dp_from_module(bot_app)
    else:
        logger.info("bot_app.init_bot not found -> falling back to module-level bot/dp.")
        bot, dp = _get_bot_and_dp_from_module(bot_app)

    if dp and not bot:
        bot_from_dp = safe_getattr(dp, "bot", None)
        if bot_from_dp:
            bot = bot_from_dp
            logger.info("Discovered bot via dp.bot")
    return bot, dp

# --------------------
async def _polling_task_runner(dp, app: web.Application):
    logger.info("Polling runner: starting aiogram dp.start_polling()")
    try:
        await dp.start_polling()
    except TerminatedByOtherGetUpdates as ex:
        logger.error("Polling terminated: TerminatedByOtherGetUpdates (another getUpdates/polling instance exists).")
        logger.debug("TerminatedByOtherGetUpdates exception: %s", ex)
    except asyncio.CancelledError:
        logger.info("Polling runner: cancelled. Attempting graceful dp.stop_polling()")
        try:
            stop_fn = safe_getattr(dp, "stop_polling", None)
            if callable(stop_fn):
                maybe = stop_fn()
                await maybe_await(maybe)
                logger.debug("dp.stop_polling() awaited successfully.")
        except Exception as ex:
            logger.debug("dp.stop_polling() failed during cancellation: %s", ex)
    except Exception as ex:
        _log_exception(ex, "Unhandled exception in polling runner")
    finally:
        logger.info("Polling runner: finished (cleaning up)")
        app["polling_task"] = None

# --------------------
async def _stop_scheduler_if_any():
    try:
        scheduler = safe_getattr(bot_app, "scheduler", None)
        if not scheduler:
            logger.info("No scheduler found in bot_app (skipping scheduler shutdown).")
            return
        loop_attr_names = ("_eventloop", "_event_loop", "_asyncio_loop", "_loop", "event_loop")
        found_loop = None
        found_attr = None
        for name in loop_attr_names:
            if hasattr(scheduler, name):
                found_loop = getattr(scheduler, name)
                found_attr = name
                break
        if found_loop is None:
            logger.info("Scheduler exists but no associated event loop found on attributes %s; skipping shutdown.", loop_attr_names)
            return
        try:
            logger.info("Stopping scheduler (bot_app.scheduler) ... (loop attr: %s)", found_attr)
            maybe_shutdown = scheduler.shutdown(wait=False)
            if asyncio.iscoroutine(maybe_shutdown):
                await maybe_shutdown
            logger.info("Scheduler stopped.")
        except Exception as ex:
            logger.exception("Failed to shutdown scheduler cleanly: %s", ex)
    except Exception as ex:
        logger.exception("Unexpected error while checking/stopping scheduler: %s", ex)

# --------------------
async def _close_bot_session(bot_obj):
    if bot_obj is None:
        return
    try:
        close_fn = safe_getattr(bot_obj, "close", None)
        if callable(close_fn):
            try:
                maybe = close_fn()
                await maybe_await(maybe)
                logger.info("Called bot.close()")
            except Exception as ex:
                logger.debug("bot.close() failed: %s", ex)
        ses = None
        get_sess = safe_getattr(bot_obj, "get_session", None)
        if callable(get_sess):
            try:
                maybe_s = get_sess()
                ses = await maybe_await(maybe_s)
            except Exception as ex:
                logger.debug("bot.get_session() failed: %s", ex)
                ses = None
        if ses is None:
            ses = safe_getattr(bot_obj, "session", None)
        if ses is not None:
            try:
                closed_attr = getattr(ses, "closed", None)
                if closed_attr is False:
                    await ses.close()
                    logger.info("Closed bot.session.")
                else:
                    logger.debug("Bot session already closed.")
            except Exception as ex:
                logger.debug("Error while closing bot session: %s", ex)
    except Exception as ex:
        logger.exception("Failed to close bot object cleanly: %s", ex)

# --------------------
async def _close_db_if_any():
    try:
        conn = safe_getattr(bot_app, "conn", None)
        if conn:
            try:
                logger.info("Closing DB connection (bot_app.conn) ...")
                try:
                    conn.commit()
                except Exception:
                    pass
                conn.close()
                logger.info("DB connection closed.")
            except Exception as ex:
                logger.exception("Failed to close DB connection: %s", ex)
        else:
            logger.debug("No bot_app.conn found (skipping DB close).")
    except Exception as ex:
        logger.exception("Unexpected error while closing DB: %s", ex)

# --------------------
async def _heartbeat_task():
    logger.info("Heartbeat task started (interval: %ss)", HEARTBEAT_INTERVAL)
    try:
        while True:
            logger.debug("heartbeat: app alive (uptime %s s)", ( _dt.datetime.utcnow() - _dt.datetime.fromisoformat(app["boot_time"]) ).total_seconds())
            await asyncio.sleep(HEARTBEAT_INTERVAL)
    except asyncio.CancelledError:
        logger.info("Heartbeat task cancelled")
    except Exception as ex:
        logger.exception("Heartbeat encountered exception: %s", ex)
    finally:
        logger.info("Heartbeat task finished")

# --------------------
async def on_startup(a: web.Application):
    logger.info("on_startup: begin")
    a["shutting_down"] = False
    if a.get("client_session") is None:
        try:
            session = create_client_session()
            a["client_session"] = session
        except Exception as ex:
            _log_exception(ex, "Failed to create shared ClientSession")
            a["client_session"] = None
    session = a.get("client_session")
    bot, dp = await _init_bot_with_session_or_fallback(session)
    a["bot"] = bot
    a["dp"] = dp

    if _has_attr(bot_app, "init_app_for_runtime"):
        try:
            maybe = bot_app.init_app_for_runtime(a)
            await maybe_await(maybe)
            logger.info("bot_app.init_app_for_runtime finished")
        except Exception as ex:
            _log_exception(ex, "bot_app.init_app_for_runtime failed (continuing)")
    else:
        logger.info("bot_app.init_app_for_runtime not found (skipping)")

    # webhook set attempt (if configured)
    try:
        if WEBHOOK_URL and bot:
            token = getattr(bot, "token", None) or os.environ.get("BOT_TOKEN")
            if not token:
                logger.warning("WEBHOOK_URL set but bot token not found; skipping automatic set_webhook.")
            else:
                path = f"{WEBHOOK_PREFIX}/{token}"
                full_url = WEBHOOK_URL.rstrip("/") + path
                try:
                    set_hook_fn = safe_getattr(bot, "set_webhook", None)
                    if callable(set_hook_fn):
                        logger.info("Setting webhook to %s ...", full_url)
                        maybe = set_hook_fn(full_url)
                        res = await maybe_await(maybe)
                        logger.info("Webhook set result: %s", res)
                        a["webhook_set_by_main"] = True
                        a["webhook_full_url"] = full_url
                    else:
                        logger.warning("bot.set_webhook not available on this Bot instance; skipping.")
                except Exception as ex:
                    logger.exception("Failed to set webhook: %s", ex)
        else:
            logger.info("WEBHOOK_URL not provided or bot missing: automatic webhook not set.")
    except Exception as ex:
        logger.exception("Unexpected error during webhook set attempt: %s", ex)

    try:
        hb = asyncio.create_task(_heartbeat_task(), name="heartbeat")
        a["heartbeat_task"] = hb
    except Exception as ex:
        logger.debug("Failed to start heartbeat task: %s", ex)

    if RUN_POLLING:
        if dp is None:
            logger.error("RUN_POLLING enabled but dispatcher (dp) is None. Polling will NOT start.")
        else:
            logger.info("RUN_POLLING enabled -> starting polling background task")
            t = asyncio.create_task(_polling_task_runner(dp, a), name="polling-runner")
            a["polling_task"] = t
    else:
        logger.info("RUN_POLLING not enabled -> dispatcher polling disabled (webhooks expected).")

    logger.info("on_startup: done")

# --------------------
async def on_cleanup(a: web.Application):
    logger.info("on_cleanup: begin (SHUTDOWN_REQUESTED=%s)", SHUTDOWN_REQUESTED)
    a["shutting_down"] = True  # internal marker only

    # If cleanup was triggered but no external signal requested shutdown, skip destructive cleanup.
    # This prevents the bot/session/DB from being closed on transient runner cleanups (which cause "bot goes silent").
    if not SHUTDOWN_REQUESTED:
        logger.warning("on_cleanup invoked but no external shutdown signal detected -> skipping destructive cleanup to keep bot alive.")
        a["cleanup_skipped_due_no_signal"] = True
        # Still stop heartbeat (non-destructive) so we don't leave that task dangling between restarts
        try:
            hb = a.get("heartbeat_task")
            if hb:
                hb.cancel()
                try:
                    await hb
                except Exception:
                    pass
        except Exception:
            pass

        # don't close bot/session/db/scheduler here — return promptly
        logger.info("on_cleanup: minimal cleanup done (heartbeat stopped). Full cleanup deferred until real shutdown.")
        return

    # ---------- If we get here, SHUTDOWN_REQUESTED == True -> perform full cleanup ----------
    # Diagnostic: list a few asyncio tasks to help understand why cleanup triggered
    try:
        tasks = asyncio.all_tasks()
        logger.debug("Currently %s asyncio tasks (showing up to 20):", len(tasks))
        for t in list(tasks)[:20]:
            try:
                name = t.get_name() if hasattr(t, "get_name") else repr(t)
                logger.debug("Task name=%s done=%s cancelled=%s repr=%s", name, t.done(), t.cancelled(), t)
            except Exception:
                logger.debug("Task repr failed: %s", t)
    except Exception:
        logger.exception("Failed to enumerate asyncio tasks during cleanup")

    # Stop heartbeat
    try:
        hb = a.get("heartbeat_task")
        if hb:
            hb.cancel()
            try:
                await hb
            except Exception:
                pass
    except Exception:
        pass

    # Stop polling task
    polling_task = a.get("polling_task")
    dp = a.get("dp")
    if polling_task is not None:
        if not polling_task.done():
            logger.info("Stopping polling task ...")
            try:
                if dp:
                    stop_fn = safe_getattr(dp, "stop_polling", None)
                    if callable(stop_fn):
                        try:
                            maybe = stop_fn()
                            await maybe_await(maybe)
                            logger.debug("dp.stop_polling() called successfully.")
                        except Exception as ex:
                            logger.debug("dp.stop_polling() raised: %s", ex)
            except Exception as ex:
                logger.debug("dp.stop_polling() raised during cleanup: %s", ex)
            try:
                polling_task.cancel()
                await asyncio.wait_for(polling_task, timeout=POLLING_SHUTDOWN_TIMEOUT)
                logger.info("Polling task finished after cancellation.")
            except asyncio.TimeoutError:
                logger.warning("Polling task did not finish in %s seconds - it will be left to cancel.", POLLING_SHUTDOWN_TIMEOUT)
            except Exception as ex:
                logger.debug("Exception while cancelling polling task: %s", ex)
        else:
            logger.info("Polling task already done.")
    else:
        logger.debug("No polling task set on app (nothing to stop).")

    # Let bot_app shutdown if present
    if _has_attr(bot_app, "shutdown_bot"):
        try:
            logger.info("Calling bot_app.shutdown_bot() ...")
            maybe = bot_app.shutdown_bot()
            await maybe_await(maybe)
            logger.info("bot_app.shutdown_bot completed.")
        except Exception as ex:
            logger.exception("bot_app.shutdown_bot failed: %s", ex)
    else:
        logger.info("bot_app.shutdown_bot not found (skipping).")

    # Attempt to stop scheduler
    try:
        await _stop_scheduler_if_any()
    except Exception as ex:
        logger.debug("Scheduler shutdown attempt raised: %s", ex)

    # Close bot session
    try:
        bot_obj = a.get("bot")
        if bot_obj is not None:
            await _close_bot_session(bot_obj)
        else:
            bot_mod = safe_getattr(bot_app, "bot", None)
            if bot_mod is not None:
                await _close_bot_session(bot_mod)
    except Exception as ex:
        logger.debug("Error closing aiogram Bot: %s", ex)

    # Close DB
    try:
        await _close_db_if_any()
    except Exception as ex:
        logger.debug("DB close attempt raised: %s", ex)

    # Close client session
    sess = a.get("client_session")
    if sess is not None:
        try:
            if not getattr(sess, "closed", True):
                logger.info("Closing shared ClientSession ...")
                await sess.close()
                logger.info("Shared ClientSession closed.")
            else:
                logger.info("Shared ClientSession already closed.")
        except Exception as ex:
            logger.exception("Failed to close shared ClientSession: %s", ex)
    else:
        logger.debug("No shared ClientSession to close.")

    logger.info("on_cleanup: done (full cleanup)")

# --------------------
async def _telegram_webhook(request: web.Request):
    token_in_path = request.match_info.get("token")
    dp = app.get("dp")
    bot_obj = app.get("bot") or safe_getattr(bot_app, "bot", None)
    if dp is None:
        logger.warning("Webhook received but dispatcher not ready -> 503")
        return web.Response(status=503, text="dispatcher not initialized")

    try:
        data = await request.json()
    except Exception:
        try:
            data_text = await request.text()
            data = json.loads(data_text or "{}")
        except Exception:
            logger.exception("Failed to parse webhook JSON")
            return web.Response(status=200, text="bad json")

    # Optional lightweight logging of payload size (avoid full dump in prod)
    try:
        logger.debug("Incoming webhook token=%s payload_keys=%s", token_in_path, list(data.keys()) if isinstance(data, dict) else None)
    except Exception:
        pass

    try:
        upd = aiogram_types.Update(**data)
    except Exception:
        try:
            upd = aiogram_types.Update.to_object(data)
        except Exception:
            logger.exception("Failed to construct Update from payload")
            return web.Response(status=200, text="bad update")

    try:
        # set current Dispatcher and Bot for this task so FSM methods can find them
        try:
            from aiogram.dispatcher.dispatcher import Dispatcher
            Dispatcher.set_current(dp)
        except Exception:
            try:
                dp.set_current(dp)
            except Exception:
                logger.debug("Could not set Dispatcher current (ignored).")

        if bot_obj:
            try:
                from aiogram import Bot
                Bot.set_current(bot_obj)
            except Exception:
                try:
                    bot_obj.set_current(bot_obj)
                except Exception:
                    logger.debug("Could not set Bot current (ignored).")

        try:
            await dp.process_update(upd)
        except Exception as ex:
            logger.exception("dp.process_update failed (caught): %s", ex)
            return web.Response(text="OK")
        return web.Response(text="OK")
    finally:
        try:
            from aiogram.dispatcher.dispatcher import Dispatcher
            Dispatcher.set_current(None)
        except Exception:
            pass
        try:
            from aiogram import Bot
            Bot.set_current(None)
        except Exception:
            pass

# --------------------
async def index(request):
    return web.Response(text="OK", content_type="text/plain")
async def ping(request):
    return web.Response(text="pong", content_type="text/plain")
async def info(request):
    data = {
        "status": "ok",
        "boot_time": app.get("boot_time"),
        "run_polling": RUN_POLLING,
        "has_polling_task": bool(app.get("polling_task")),
        "bot_present": bool(app.get("bot")),
        "dp_present": bool(app.get("dp")),
        "webhook_set_by_main": bool(app.get("webhook_set_by_main")),
        "webhook_full_url": app.get("webhook_full_url"),
        "cleanup_skipped_due_no_signal": app.get("cleanup_skipped_due_no_signal", False),
    }
    return web.json_response(data)

# --------------------
def _loop_exception_handler(loop, context):
    try:
        msg = context.get("exception") or context.get("message")
        logger.error("Unhandled loop exception: %s", msg)
        if "exception" in context and context["exception"] is not None:
            logger.debug("".join(traceback.format_exception(type(context["exception"]), context["exception"], context["exception"].__traceback__)))
    except Exception:
        logger.exception("Error in loop exception handler")

def _install_signal_handlers(loop: asyncio.AbstractEventLoop):
    def _on_signal(signame):
        global SHUTDOWN_REQUESTED
        SHUTDOWN_REQUESTED = True
        logger.info("Signal handler fired: %s (SHUTDOWN_REQUESTED set True).", signame)
        try:
            asyncio.create_task(_signal_trigger_shutdown(signame))
        except Exception:
            logger.exception("Failed to create _signal_trigger_shutdown task")

    try:
        for signame in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, signame)
            loop.add_signal_handler(sig, lambda s=signame: _on_signal(s))
        logger.info("Signal handlers installed for SIGINT/SIGTERM.")
    except NotImplementedError:
        logger.warning("add_signal_handler not supported on this platform; OS signals will not be handled specially.")

async def _signal_trigger_shutdown(signame: str):
    logger.info("Received signal %s - aiohttp runner should begin graceful shutdown.", signame)
    await asyncio.sleep(0.01)

# --------------------
app.router.add_get("/", index)
app.router.add_get("/ping", ping)
app.router.add_head("/", index)
app.router.add_get("/info", info)
app.router.add_post(f"{WEBHOOK_PREFIX}/{{token:.*}}", _telegram_webhook)
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)

# --------------------
def _run_once():
    try:
        web.run_app(app, host="0.0.0.0", port=PORT)
        if SHUTDOWN_REQUESTED:
            logger.info("web.run_app finished and SHUTDOWN_REQUESTED is True => normal deliberate shutdown.")
            return True, None
        else:
            logger.warning("web.run_app exited normally but no shutdown signal was recorded (unexpected).")
            # Return False so the outer loop may restart the app (auto-restart behavior)
            return False, "web.run_app exited without shutdown signal"
    except SystemExit as ex:
        logger.warning("web.run_app exited via SystemExit: %s", ex)
        return False, ex
    except KeyboardInterrupt as ex:
        logger.warning("web.run_app interrupted by KeyboardInterrupt: %s", ex)
        return False, ex
    except Exception as ex:
        logger.exception("web.run_app raised exception (will consider restart): %s", ex)
        return False, ex

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.set_exception_handler(_loop_exception_handler)
    except Exception:
        logger.debug("Could not set custom loop exception handler (ignored).")
    _install_signal_handlers(loop)

    logger.info("Starting main process (auto_restart=%s, max=%s, delay=%ss)", AUTO_RESTART, AUTO_RESTART_MAX, AUTO_RESTART_DELAY_S)

    restart_count = 0
    while True:
        success, err = _run_once()
        if success:
            logger.info("web.run_app exited normally. Will not restart.")
            break

        restart_count += 1
        if not AUTO_RESTART:
            logger.warning("AUTO_RESTART disabled. Exiting after web.run_app exit.")
            break

        if SHUTDOWN_REQUESTED:
            logger.info("SHUTDOWN_REQUESTED is True (signal received). Not restarting.")
            break

        if restart_count > AUTO_RESTART_MAX:
            logger.error("Exceeded AUTO_RESTART_MAX (%s). Giving up and exiting.", AUTO_RESTART_MAX)
            break

        logger.warning("web.run_app exited unexpectedly (attempt %s/%s). Waiting %ss before restart. Error: %s", restart_count, AUTO_RESTART_MAX, AUTO_RESTART_DELAY_S, repr(err))
        try:
            import time
            time.sleep(AUTO_RESTART_DELAY_S)
        except Exception:
            pass
        logger.info("Restarting web.run_app now (attempt %s)", restart_count + 1)

    # final best-effort cleanup
    try:
        sess = app.get("client_session")
        if sess is not None and not getattr(sess, "closed", True):
            try:
                loop.run_until_complete(sess.close())
            except Exception:
                pass
    except Exception:
        pass

    try:
        if hasattr(bot_app, "shutdown_bot"):
            maybe = bot_app.shutdown_bot()
            if asyncio.iscoroutine(maybe):
                loop.run_until_complete(maybe)
    except Exception:
        pass

    logger.info("Main process exiting.")
