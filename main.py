# main.py
"""
Entrypoint for Financial_Guide — robust webhook-friendly version.

Key points:
- create single shared aiohttp.ClientSession
- attempt bot_app.init_bot(session) else fallback to module-level bot/dp
- set webhook automatically if WEBHOOK_URL provided
- do NOT delete webhook on cleanup unless WEBHOOK_AUTO_DELETE=1
- protect webhook handler: catch all exceptions, always return 200 to Telegram
- set loop exception handler to avoid accidental shutdown on unhandled exceptions
- graceful cleanup: stop polling (if running), stop scheduler if started here,
  close bot session, close shared ClientSession, close DB if present
"""

import os
import sys
import asyncio
import signal
import logging
import traceback
import json
from typing import Optional, Tuple, Any

from aiohttp import web, ClientSession, ClientTimeout
from aiogram.utils.exceptions import TerminatedByOtherGetUpdates
from aiogram import Bot
from aiogram.dispatcher.dispatcher import Dispatcher
from aiogram import types as aiogram_types

import bot_app

# --------------------
# Logging
# --------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

# --------------------
# Config
# --------------------
PORT = int(os.environ.get("PORT", "10000"))
RUN_POLLING = os.environ.get("RUN_POLLING", "0").lower() in ("1", "true", "yes")
CLIENT_SESSION_TIMEOUT_S = float(os.environ.get("CLIENT_SESSION_TIMEOUT_S", "60"))
POLLING_SHUTDOWN_TIMEOUT = float(os.environ.get("POLLING_SHUTDOWN_TIMEOUT", "6.0"))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # e.g. "https://financial-guide.onrender.com"
WEBHOOK_PREFIX = os.environ.get("WEBHOOK_PREFIX", "/webhook").rstrip("/")
WEBHOOK_AUTO_DELETE = os.environ.get("WEBHOOK_AUTO_DELETE", "0") in ("1", "true", "yes")

# --------------------
# App container
# --------------------
app = web.Application()
import datetime as _dt
app["boot_time"] = _dt.datetime.utcnow().isoformat()
app["polling_task"] = None
app["client_session"] = None
app["bot"] = None
app["dp"] = None
app["shutting_down"] = False
app["webhook_set_by_main"] = False
app["webhook_full_url"] = None

# --------------------
# Helpers
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
# ClientSession creator
# --------------------
def create_client_session() -> ClientSession:
    timeout = ClientTimeout(total=CLIENT_SESSION_TIMEOUT_S)
    session = ClientSession(timeout=timeout)
    logger.info("Created shared aiohttp ClientSession (timeout=%ss)", CLIENT_SESSION_TIMEOUT_S)
    return session

# --------------------
# Bot init / fallback
# --------------------
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
                logger.info("bot_app.init_bot returned None; using module-level bot/dp (if present).")
            else:
                bot = res
                dp = getattr(bot_app, "dp", None)
                logger.info("bot_app.init_bot returned object; interpreted as bot. dp=%s", bool(dp))
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
# Polling runner
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
# Scheduler safe stop
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
# Close bot session
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
                    logger.info("Closed bot session.")
                else:
                    logger.debug("Bot session already closed.")
            except Exception as ex:
                logger.debug("Error while closing bot session: %s", ex)
    except Exception as ex:
        logger.exception("Failed to close bot object cleanly: %s", ex)

# --------------------
# DB close
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
# Startup
# --------------------
async def on_startup(app: web.Application):
    logger.info("on_startup: begin")
    # create client session
    if app.get("client_session") is None:
        try:
            session = create_client_session()
            app["client_session"] = session
        except Exception as ex:
            _log_exception(ex, "Failed to create shared ClientSession")
            app["client_session"] = None
    else:
        logger.info("on_startup: app already had client_session")

    session = app.get("client_session")
    bot, dp = await _init_bot_with_session_or_fallback(session)
    app["bot"] = bot
    app["dp"] = dp

    # let bot_app init runtime (scheduler, locks)
    if _has_attr(bot_app, "init_app_for_runtime"):
        try:
            maybe = bot_app.init_app_for_runtime(app)
            await maybe_await(maybe)
            logger.info("bot_app.init_app_for_runtime finished")
        except Exception as ex:
            _log_exception(ex, "bot_app.init_app_for_runtime failed (continuing)")
    else:
        logger.info("bot_app.init_app_for_runtime not found (skipping)")

    # set webhook if requested and bot present
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
                        app["webhook_set_by_main"] = True
                        app["webhook_full_url"] = full_url
                    else:
                        logger.warning("bot.set_webhook not available on this Bot instance; skipping.")
                except Exception as ex:
                    logger.exception("Failed to set webhook: %s", ex)
        else:
            logger.info("WEBHOOK_URL not provided or bot missing: automatic webhook not set.")
    except Exception as ex:
        logger.exception("Unexpected error during webhook set attempt: %s", ex)

    # polling
    if RUN_POLLING:
        if dp is None:
            logger.error("RUN_POLLING enabled but dispatcher (dp) is None. Polling will NOT start.")
        else:
            logger.info("RUN_POLLING enabled -> starting polling background task")
            t = asyncio.create_task(_polling_task_runner(dp, app), name="polling-runner")
            app["polling_task"] = t
    else:
        logger.info("RUN_POLLING not enabled -> dispatcher polling disabled (webhooks expected).")

    logger.info("on_startup: done")

# --------------------
# Cleanup
# --------------------
async def on_cleanup(app: web.Application):
    logger.info("on_cleanup: begin")
    try:
        app["shutting_down"] = True
    except Exception:
        logger.debug("Could not set app['shutting_down'] (ignored)")

    # stop polling task
    polling_task = app.get("polling_task")
    dp = app.get("dp")
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
                logger.debug("dp.stop_polling() failed during cleanup: %s", ex)
            try:
                polling_task.cancel()
                await asyncio.wait_for(polling_task, timeout=POLLING_SHUTDOWN_TIMEOUT)
                logger.info("Polling task finished after cancellation.")
            except asyncio.TimeoutError:
                logger.warning("Polling task did not finish in %s seconds - left to cancel.", POLLING_SHUTDOWN_TIMEOUT)
            except Exception as ex:
                logger.debug("Exception while cancelling polling task: %s", ex)
        else:
            logger.info("Polling task already done.")
    else:
        logger.debug("No polling task set on app (nothing to stop).")

    # call bot_app.shutdown_bot if exists
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

    # attempt scheduler stop
    try:
        await _stop_scheduler_if_any()
    except Exception as ex:
        logger.debug("Scheduler shutdown attempt raised: %s", ex)

    # delete webhook only if we set it and only if auto-delete permitted
    try:
        if app.get("webhook_set_by_main") and WEBHOOK_AUTO_DELETE:
            bot_obj = app.get("bot")
            del_hook = safe_getattr(bot_obj, "delete_webhook", None) or safe_getattr(bot_obj, "remove_webhook", None)
            if callable(del_hook):
                try:
                    await maybe_await(del_hook())
                    logger.info("Webhook deleted on cleanup.")
                except Exception as ex:
                    logger.debug("Failed to delete webhook on cleanup: %s", ex)
        else:
            if app.get("webhook_set_by_main"):
                logger.info("Webhook was set by main, but WEBHOOK_AUTO_DELETE is false -> keeping webhook.")
    except Exception as ex:
        logger.debug("Error during webhook deletion attempt: %s", ex)

    # close bot session
    try:
        bot_obj = app.get("bot")
        if bot_obj is not None:
            await _close_bot_session(bot_obj)
        else:
            bot_mod = safe_getattr(bot_app, "bot", None)
            if bot_mod is not None:
                await _close_bot_session(bot_mod)
    except Exception as ex:
        logger.debug("Error closing aiogram Bot: %s", ex)

    # close DB
    try:
        await _close_db_if_any()
    except Exception as ex:
        logger.debug("DB close attempt raised: %s", ex)

    # close client session
    sess = app.get("client_session")
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

    logger.info("on_cleanup: done")

# --------------------
# Webhook handler (very defensive)
# --------------------
async def _telegram_webhook(request: web.Request):
    token_in_path = request.match_info.get("token")
    dp = app.get("dp")
    bot_obj = app.get("bot") or safe_getattr(bot_app, "bot", None)
    if dp is None:
        logger.warning("Webhook received but dispatcher not ready -> 503")
        return web.Response(status=503, text="dispatcher not initialized")

    # parse JSON defensively
    try:
        data = await request.json()
    except Exception:
        try:
            data_text = await request.text()
            data = json.loads(data_text or "{}")
        except Exception:
            logger.exception("Failed to parse webhook JSON")
            # return 200 so Telegram won't spam retries; but indicate bad payload
            return web.Response(status=200, text="bad json")

    # build Update defensively
    try:
        upd = aiogram_types.Update(**data)
    except Exception:
        try:
            # fallback: try to construct via to_object or leave raw
            upd = aiogram_types.Update.to_object(data)
        except Exception:
            logger.exception("Failed to construct Update from payload")
            return web.Response(status=200, text="bad update")

    # Ensure dispatcher/bot current context is set for FSM and handlers
    try:
        try:
            Dispatcher.set_current(dp)
        except Exception:
            try:
                dp.set_current(dp)
            except Exception:
                logger.debug("Could not set Dispatcher current (ignored).")
        if bot_obj:
            try:
                Bot.set_current(bot_obj)
            except Exception:
                try:
                    bot_obj.set_current(bot_obj)
                except Exception:
                    logger.debug("Could not set Bot current (ignored).")

        # Process update inside try/except so exceptions won't crash the process
        try:
            await dp.process_update(upd)
        except Exception as ex:
            # Log error but DON'T re-raise — return 200 to Telegram
            logger.exception("dp.process_update failed (caught): %s", ex)
            # Optionally, you could persist the update or notify admin here
            return web.Response(text="OK")
        return web.Response(text="OK")
    finally:
        # unset contexts quietly
        try:
            Dispatcher.set_current(None)
        except Exception:
            pass
        try:
            Bot.set_current(None)
        except Exception:
            pass

# --------------------
# Health endpoints
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
    }
    return web.json_response(data)

# --------------------
# Signal handlers & loop exception handler
# --------------------
def _install_signal_handlers(loop: asyncio.AbstractEventLoop):
    try:
        for signame in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, signame)
            loop.add_signal_handler(sig, lambda s=signame: asyncio.create_task(_signal_trigger_shutdown(s)))
        logger.info("Signal handlers installed for SIGINT/SIGTERM.")
    except NotImplementedError:
        logger.warning("add_signal_handler not supported on this platform; OS signals will not be handled specially.")

async def _signal_trigger_shutdown(signame: str):
    logger.info("Received signal %s - initiating graceful shutdown via aiohttp runner.", signame)
    await asyncio.sleep(0.01)

def _loop_exception_handler(loop, context):
    """
    log unhandled exceptions and DO NOT let them crash the loop
    (prevents web.run_app from unwinding on a single unexpected error).
    """
    try:
        msg = context.get("exception") or context.get("message")
        logger.error("Unhandled loop exception: %s", msg)
        # print traceback if available
        if "exception" in context and context["exception"] is not None:
            logger.debug("".join(traceback.format_exception(type(context["exception"]), context["exception"], context["exception"].__traceback__)))
    except Exception:
        logger.exception("Error in loop exception handler")

# --------------------
# Routes and hooks registration
# --------------------
app.router.add_get("/", index)
app.router.add_get("/ping", ping)
app.router.add_head("/", index)
app.router.add_get("/info", info)
app.router.add_post(f"{WEBHOOK_PREFIX}/{{token:.*}}", _telegram_webhook)
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)

# --------------------
# Entrypoint
# --------------------
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    # install custom loop exception handler to avoid accidental shutdowns
    try:
        loop.set_exception_handler(_loop_exception_handler)
    except Exception:
        logger.debug("Could not set custom loop exception handler (ignored).")

    _install_signal_handlers(loop)
    logger.info("Starting web app on 0.0.0.0:%s", PORT)
    try:
        web.run_app(app, host="0.0.0.0", port=PORT)
    except Exception as ex:
        logger.exception("web.run_app raised exception (app exiting): %s", ex)
    finally:
        # final cleanup attempts (best-effort)
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
