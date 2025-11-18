# main.py
"""
Robust entrypoint for Financial_Guide (fixed webhook/context issues)

Key fixes:
- provide /webhook/{token} handler that sets Dispatcher/Bot "current" context
  so FSM (State.set / get_current) works under webhook processing.
- safer scheduler shutdown (guard when scheduler._eventloop is None)
- safer closure of bot session (try get_session(), fallback to .session)
- defensive handling if bot_app.init_bot/init_app_for_runtime absent
- register webhook route so Telegram requests are accepted (200/503 as appropriate)
- abundant logging
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

# aiogram types/classes we'll use directly
from aiogram import Bot
from aiogram.dispatcher.dispatcher import Dispatcher

# Import the user's bot_app module (contains handlers, scheduler, db, etc.)
import bot_app

# ----------------------
# Logging configuration
# ----------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

# ----------------------
# Configuration
# ----------------------
PORT = int(os.environ.get("PORT", "10000"))
# RUN_POLLING must be explicitly enabled for polling to start (safe default: False).
RUN_POLLING = os.environ.get("RUN_POLLING", "0").lower() in ("1", "true", "yes")
# How long to wait when stopping polling before force-cancelling
POLLING_SHUTDOWN_TIMEOUT = float(os.environ.get("POLLING_SHUTDOWN_TIMEOUT", "6.0"))
# Shared client session timeout
CLIENT_SESSION_TIMEOUT_S = float(os.environ.get("CLIENT_SESSION_TIMEOUT_S", "60"))
# webhook path prefix (if your bot sets webhook to a different path - change here)
WEBHOOK_PREFIX = os.environ.get("WEBHOOK_PREFIX", "/webhook")

# ----------------------
# Application container
# ----------------------
app = web.Application()
import datetime as _dt
app["boot_time"] = _dt.datetime.utcnow().isoformat()
app["polling_task"] = None
app["client_session"] = None
app["bot"] = None
app["dp"] = None
app["shutting_down"] = False

# ----------------------
# Helper utilities
# ----------------------


def safe_getattr(obj: Any, name: str, default: Any = None):
    """Safe getattr wrapper that returns default if attribute missing or raises."""
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


async def maybe_await(obj):
    """
    Await obj if it's awaitable (coroutine), otherwise return it.
    """
    if asyncio.iscoroutine(obj):
        return await obj
    return obj


def _log_exception(exc: Optional[BaseException] = None, msg: str = "Exception"):
    if exc is None:
        logger.exception(msg)
    else:
        logger.error("%s: %s", msg, exc)
        logger.debug("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))


# ----------------------
# Bot / Dispatcher helpers
# ----------------------


def create_client_session() -> ClientSession:
    """
    Create a shared aiohttp.ClientSession for the web app and (optionally) for the aiogram Bot.
    """
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
    """
    Attempt to obtain bot and dp from the module attributes (fallback).
    """
    bot = safe_getattr(module, "bot", None)
    dp = safe_getattr(module, "dp", None)
    if bot and dp:
        logger.info("Found bot and dp at module-level (fallback).")
    else:
        logger.info("Module-level bot/dp not found (fallback may be incomplete). bot=%s dp=%s", bool(bot), bool(dp))
    return bot, dp


async def _init_bot_with_session_or_fallback(session: Optional[ClientSession]):
    """
    Try to call bot_app.init_bot(session) if present. If not present or it fails,
    use module-level bot/dp (bot_app.bot, bot_app.dp) as a fallback.
    """
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

    # final sanity: if dp is present but bot is None, try to get bot from dp
    if dp and not bot:
        bot_from_dp = safe_getattr(dp, "bot", None)
        if bot_from_dp:
            bot = bot_from_dp
            logger.info("Discovered bot via dp.bot")

    return bot, dp


# ----------------------
# Polling runner wrapper
# ----------------------


async def _polling_task_runner(dp, app: web.Application):
    """
    Background task that runs dp.start_polling() and handles TerminatedByOtherGetUpdates.
    """
    logger.info("Polling runner: starting aiogram dp.start_polling()")
    try:
        await dp.start_polling()
    except TerminatedByOtherGetUpdates as ex:
        logger.error(
            "Polling terminated: TerminatedByOtherGetUpdates: another getUpdates/polling instance exists for this bot token. "
            "Make sure only one polling instance runs, or switch to webhooks."
        )
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


# ----------------------
# Startup / Cleanup
# ----------------------


async def on_startup(app: web.Application):
    logger.info("on_startup: begin")

    # Create shared client session if not already present
    if app.get("client_session") is None:
        try:
            session = create_client_session()
            app["client_session"] = session
        except Exception as ex:
            _log_exception(ex, "Failed to create shared ClientSession")
            app["client_session"] = None
    else:
        logger.info("on_startup: app already had client_session")

    # Try to initialize bot and dispatcher using the session (if available)
    session = app.get("client_session")
    bot, dp = await _init_bot_with_session_or_fallback(session)
    app["bot"] = bot
    app["dp"] = dp

    # Allow bot_app to initialize runtime resources if function available
    if _has_attr(bot_app, "init_app_for_runtime"):
        try:
            maybe = bot_app.init_app_for_runtime(app)
            await maybe_await(maybe)
            logger.info("bot_app.init_app_for_runtime finished")
        except Exception as ex:
            _log_exception(ex, "bot_app.init_app_for_runtime failed (continuing)")
    else:
        logger.info("bot_app.init_app_for_runtime not found (skipping)")

    # If RUN_POLLING enabled, start polling in background task.
    if RUN_POLLING:
        if dp is None:
            logger.error("RUN_POLLING is enabled but dispatcher (dp) is None. Polling will NOT start.")
        else:
            logger.info("RUN_POLLING enabled -> starting polling background task")
            t = asyncio.create_task(_polling_task_runner(dp, app), name="polling-runner")
            app["polling_task"] = t
    else:
        logger.info("RUN_POLLING not enabled -> dispatcher polling disabled. Use RUN_POLLING=1 for local single-instance polling.")

    logger.info("on_startup: done")


async def _stop_scheduler_if_any():
    """
    Attempt to stop APScheduler / scheduler stored in bot_app.scheduler or similar.
    Guard against scheduler._eventloop is None (created elsewhere).
    """
    try:
        scheduler = safe_getattr(bot_app, "scheduler", None)
        if not scheduler:
            logger.info("No scheduler found in bot_app (skipping scheduler shutdown).")
            return

        # common attribute names that may point to event loop
        loop_attr_names = ("_eventloop", "_event_loop", "_asyncio_loop", "_loop", "event_loop")
        found_loop = None
        found_attr = None
        for name in loop_attr_names:
            if hasattr(scheduler, name):
                found_loop = getattr(scheduler, name)
                found_attr = name
                break

        if found_loop is None:
            logger.info(
                "Scheduler exists but no associated event loop found on attributes %s; "
                "skipping shutdown to avoid AttributeError.",
                loop_attr_names,
            )
            return

        try:
            logger.info("Stopping scheduler (bot_app.scheduler) ... (loop attr: %s)", found_attr)
            maybe_shutdown = scheduler.shutdown(wait=False)
            if asyncio.iscoroutine(maybe_shutdown):
                await maybe_shutdown
            logger.info("Scheduler stopped.")
        except AttributeError as ex:
            logger.exception("AttributeError while shutting down scheduler (likely _eventloop issue): %s", ex)
        except Exception as ex:
            logger.exception("Failed to shutdown scheduler cleanly: %s", ex)

    except Exception as ex:
        logger.exception("Unexpected error while checking/stopping scheduler: %s", ex)


async def _close_bot_session(bot_obj):
    """
    Close aiogram.Bot resources cleanly.
    Try await bot.get_session() first, fallback to bot.session, and call bot.close().
    """
    if bot_obj is None:
        return
    try:
        # try close()
        close_fn = safe_getattr(bot_obj, "close", None)
        if callable(close_fn):
            try:
                maybe = close_fn()
                await maybe_await(maybe)
                logger.info("Called bot.close()")
            except Exception as ex:
                logger.debug("bot.close() call failed: %s", ex)

        # try get_session()
        ses = None
        get_sess = safe_getattr(bot_obj, "get_session", None)
        if callable(get_sess):
            try:
                maybe_s = get_sess()
                ses = await maybe_await(maybe_s)
            except Exception as ex:
                logger.debug("bot.get_session() failed or not supported: %s", ex)
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
                    logger.debug("Bot session is already closed or 'closed' attribute truthy.")
            except Exception as ex:
                logger.debug("Error while closing bot's session: %s", ex)
    except Exception as ex:
        logger.exception("Failed to close bot object cleanly: %s", ex)


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


async def on_cleanup(app: web.Application):
    logger.info("on_cleanup: begin")
    # mark shutting down
    try:
        app["shutting_down"] = True
    except Exception:
        # older aiohttp warns when changing state; ignore
        logger.debug("Could not set app['shutting_down'] (ignored)")

    # Stop polling task if running
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

    # Let bot_app shutdown if available
    if _has_attr(bot_app, "shutdown_bot"):
        try:
            logger.info("Calling bot_app.shutdown_bot() ...")
            maybe = bot_app.shutdown_bot()
            await maybe_await(maybe)
            logger.info("bot_app.shutdown_bot completed.")
        except Exception as ex:
            logger.exception("bot_app.shutdown_bot failed: %s", ex)
    else:
        logger.info("bot_app.shutdown_bot not found (skipping). We'll attempt individual cleanup steps.")

    # Attempt to stop scheduler
    try:
        await _stop_scheduler_if_any()
    except Exception as ex:
        logger.debug("Scheduler shutdown attempt raised: %s", ex)

    # Close bot session/object
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

    # Close DB if present
    try:
        await _close_db_if_any()
    except Exception as ex:
        logger.debug("DB close attempt raised: %s", ex)

    # Close client session created by this main if present
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


# ----------------------
# Webhook handler for Telegram
# ----------------------
# This is the crucial addition: Telegram will POST updates to /webhook/{token}.
# We parse JSON into aiogram.types.Update and call dp.process_update(update).
# VERY IMPORTANT: before calling dp.process_update we set Dispatcher/Bot "current"
# so FSM/State APIs that rely on Dispatcher.get_current() / Bot.get_current() work.
#
# We attempt to set and then reset contextvars to avoid leaking state between requests.

from aiogram import types as aiogram_types  # imported here to use Update type


async def _telegram_webhook(request: web.Request):
    """
    POST /webhook/{token}
    """
    token_in_path = request.match_info.get("token")
    dp = app.get("dp")
    bot_obj = app.get("bot") or safe_getattr(bot_app, "bot", None)

    if dp is None:
        logger.warning("Received webhook but dispatcher not ready -> 503")
        return web.Response(status=503, text="dispatcher not initialized")

    # parse payload as json
    try:
        data = await request.json()
    except Exception:
        try:
            text = await request.text()
            data = json.loads(text or "{}")
        except Exception:
            logger.exception("Failed to parse incoming webhook JSON")
            return web.Response(status=400, text="bad request")

    # Build Update object
    try:
        upd = aiogram_types.Update(**data)
    except Exception:
        # fallback: use from_object where available, or pass data through
        try:
            upd = aiogram_types.Update.to_object(data)
        except Exception:
            logger.exception("Failed to construct Update from payload; returning 400")
            return web.Response(status=400, text="bad update")

    # Set context for Dispatcher and Bot so FSM and Bot.get_current() work
    # Prefer classmethod setting, but bound method exists too.
    set_dp = getattr(Dispatcher, "set_current", None)
    set_bot = getattr(Bot, "set_current", None)
    unset_dp = getattr(Dispatcher, "set_current", None)
    unset_bot = getattr(Bot, "set_current", None)

    try:
        # set current dispatcher and bot (if available)
        if callable(set_dp):
            try:
                # some aiogram builds support Dispatcher.set_current(dp)
                Dispatcher.set_current(dp)
            except Exception:
                # try bound method
                try:
                    dp.set_current(dp)
                except Exception:
                    logger.debug("Could not set Dispatcher current via set_current (ignored).")
        if bot_obj and callable(set_bot):
            try:
                Bot.set_current(bot_obj)
            except Exception:
                try:
                    bot_obj.set_current(bot_obj)
                except Exception:
                    logger.debug("Could not set Bot current (ignored).")

        # Now process update
        try:
            await dp.process_update(upd)
        except Exception as ex:
            # Log inner exception but still return 200 to Telegram (so it doesn't keep retrying endlessly).
            logger.exception("dp.process_update failed: %s", ex)
        # Return OK
        return web.Response(text="OK")
    finally:
        # unset context to avoid leaking into subsequent requests
        try:
            if callable(unset_dp):
                try:
                    Dispatcher.set_current(None)
                except Exception:
                    try:
                        dp.set_current(None)
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            if callable(unset_bot):
                try:
                    Bot.set_current(None)
                except Exception:
                    try:
                        bot_obj.set_current(None)
                    except Exception:
                        pass
        except Exception:
            pass


# ----------------------
# Health / Utility routes
# ----------------------


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
    }
    return web.json_response(data)


# ----------------------
# Signal handlers / Graceful shutdown trigger
# ----------------------


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


# ----------------------
# Register routes and startup/cleanup hooks
# ----------------------
app.router.add_get("/", index)
app.router.add_get("/ping", ping)
app.router.add_head("/", index)
app.router.add_get("/info", info)
# webhook route — keep token in path for basic protection
app.router.add_post(f"{WEBHOOK_PREFIX}/{{token:.*}}", _telegram_webhook)

app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)


# ----------------------
# Entrypoint
# ----------------------
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    _install_signal_handlers(loop)

    logger.info("Starting web app on 0.0.0.0:%s", PORT)
    try:
        web.run_app(app, host="0.0.0.0", port=PORT)
    except Exception as ex:
        logger.exception("web.run_app raised exception (app exiting): %s", ex)
    finally:
        # best-effort synchronous cleanup after loop stops
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
