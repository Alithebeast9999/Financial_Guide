# main.py
"""
Robust entrypoint for Financial_Guide

Responsibilities:
- create a single shared aiohttp.ClientSession for the web app and (optionally) for aiogram bot
- try to call bot_app.init_bot(session) to allow bot_app to create its Bot/Dispatcher bound to our session
- fallback to using bot_app.bot / bot_app.dp if init_bot is not present
- call bot_app.init_app_for_runtime(app) to let bot_app initialize scheduler, db locks, etc.
- start aiogram polling only when RUN_POLLING environment var is explicitly enabled
- protect against TerminatedByOtherGetUpdates errors (caused by more than one polling instance)
- on cleanup: stop polling, stop scheduler, close bot sessions/sockets, close shared ClientSession, close DB if present
- provide health endpoints for Render / uptime monitors
- install graceful SIGINT/SIGTERM handlers so Render shutdown behaves cleanly
- abundant logging and defensive checks
"""

import os
import sys
import asyncio
import signal
import logging
import traceback
from typing import Optional, Tuple, Any, Callable

from aiohttp import web, ClientSession, ClientTimeout
from aiogram.utils.exceptions import TerminatedByOtherGetUpdates

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

# ----------------------
# Application container
# ----------------------
app = web.Application()
# We'll attach useful objects into app dict during startup:
# - app['client_session'] -> the shared aiohttp.ClientSession created here
# - app['bot'] -> aiogram.Bot instance (if available)
# - app['dp'] -> aiogram.Dispatcher instance (if available)
# - app['polling_task'] -> asyncio.Task running dp.start_polling()
# - app['shutting_down'] -> bool flag set during cleanup
# - app['boot_time'] -> string timestamp
import datetime
app["boot_time"] = datetime.datetime.utcnow().isoformat()
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
    We keep a fairly conservative timeout.
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


async def _init_bot_with_session_or_fallback(session: ClientSession):
    """
    Try to call bot_app.init_bot(session) if present. If not present or it fails,
    use module-level bot/dp (bot_app.bot, bot_app.dp) as a fallback.
    Expected return from init_bot(session) is either:
        - (bot, dp)
        - bot  (less likely)
        - a coroutine that sets up bot_app.bot/dp internally
    The function is defensive and returns tuple (bot, dp) where elements might be None.
    """
    bot = None
    dp = None

    if _has_attr(bot_app, "init_bot"):
        try:
            logger.info("Calling bot_app.init_bot(session) ...")
            maybe = bot_app.init_bot(session)
            res = await maybe_await(maybe)
            # Accept multiple shapes of return
            if isinstance(res, tuple) and len(res) >= 2:
                bot, dp = res[0], res[1]
                logger.info("bot_app.init_bot returned (bot, dp).")
            elif res is None:
                # assume bot_app created module-level objects
                bot, dp = _get_bot_and_dp_from_module(bot_app)
                logger.info("bot_app.init_bot returned None; using module-level bot/dp (if present).")
            else:
                # some code returns only bot
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
    Background task that runs dp.start_polling() and handles the TerminatedByOtherGetUpdates
    exception gracefully — that exception arises when Telegram detects multiple polling clients
    for the same token.

    Important behaviors:
    - If TerminatedByOtherGetUpdates is raised, log an error and stop the polling task.
    - On CancelledError, attempt to stop polling politely.
    - Update app['polling_task'] to None on exit so cleanup logic knows it's gone.
    """
    logger.info("Polling runner: starting aiogram dp.start_polling()")
    try:
        # dp.start_polling is blocking (async) until stopped; await it
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
            # dp.stop_polling tries to stop the long-poll loop
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
        # mark as finished
        app["polling_task"] = None


# ----------------------
# Startup / Cleanup
# ----------------------


async def on_startup(app: web.Application):
    """
    aiohttp on_startup hook. Responsibilities:
    - create shared ClientSession
    - initialize bot via bot_app.init_bot(session) or fallback
    - call bot_app.init_app_for_runtime(app)
    - optionally start polling (based on RUN_POLLING)
    """
    logger.info("on_startup: begin")

    # Create shared client session if not already present
    if app.get("client_session") is None:
        try:
            session = create_client_session()
            app["client_session"] = session
        except Exception as ex:
            _log_exception(ex, "Failed to create shared ClientSession")
            # still continue; some features might fail if session absent
            app["client_session"] = None
    else:
        logger.info("on_startup: app already had client_session")

    # Try to initialize bot and dispatcher using the session (if available)
    session = app.get("client_session")
    bot, dp = await _init_bot_with_session_or_fallback(session)
    app["bot"] = bot
    app["dp"] = dp

    # Let bot_app perform runtime initializations (scheduler jobs, db_lock). It exists in provided bot_app.
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
    It's common to have an AsyncIOScheduler instance as bot_app.scheduler.
    """
    try:
        scheduler = safe_getattr(bot_app, "scheduler", None)
        if scheduler:
            try:
                logger.info("Stopping scheduler (bot_app.scheduler) ...")
                # apscheduler's AsyncIOScheduler.shutdown() is blocking sync method; call it in thread if necessary
                maybe_shutdown = scheduler.shutdown(wait=False)
                # scheduler.shutdown may be synchronous or coroutine — handle both
                if asyncio.iscoroutine(maybe_shutdown):
                    await maybe_shutdown
                logger.info("Scheduler stopped.")
            except Exception as ex:
                logger.exception("Failed to shutdown scheduler cleanly: %s", ex)
        else:
            logger.info("No scheduler found in bot_app (skipping scheduler shutdown).")
    except Exception as ex:
        logger.exception("Unexpected error while checking scheduler: %s", ex)


async def _close_bot_session(bot_obj):
    """
    Attempt to close aiogram Bot resources cleanly.
    aiogram.Bot provides close() coroutine and session attribute referencing aiohttp.ClientSession.
    We attempt multiple ways to close it safely.
    """
    if bot_obj is None:
        return
    try:
        # Preferred: if bot has close coroutine
        close_fn = safe_getattr(bot_obj, "close", None)
        if callable(close_fn):
            maybe = close_fn()
            await maybe_await(maybe)
            logger.info("Called bot.close()")
        # Additionally, if Bot exposes 'session' and it's an aiohttp.ClientSession, close it
        ses = safe_getattr(bot_obj, "session", None)
        if ses and not getattr(ses, "closed", True):
            try:
                await ses.close()
                logger.info("Closed bot.session (aiohttp.ClientSession).")
            except Exception as ex:
                logger.debug("Closing bot.session failed: %s", ex)
    except Exception as ex:
        logger.exception("Failed to close bot object cleanly: %s", ex)


async def _close_db_if_any():
    """
    If bot_app provides a sqlite connection object 'conn', try to close it.
    If bot_app manages DB differently, skip.
    """
    try:
        conn = safe_getattr(bot_app, "conn", None)
        if conn:
            try:
                logger.info("Closing DB connection (bot_app.conn) ...")
                conn.commit()
                conn.close()
                logger.info("DB connection closed.")
            except Exception as ex:
                logger.exception("Failed to close DB connection: %s", ex)
        else:
            logger.debug("No bot_app.conn found (skipping DB close).")
    except Exception as ex:
        logger.exception("Unexpected error while closing DB: %s", ex)


async def on_cleanup(app: web.Application):
    """
    aiohttp on_cleanup hook. Responsibilities:
    - signal and stop polling task if running
    - call bot_app.shutdown_bot() if present to let bot_app cleanup its resources
    - attempt to stop scheduler
    - close bot session and module session if created here
    - close shared ClientSession
    - close DB connection if present
    """
    logger.info("on_cleanup: begin")
    app["shutting_down"] = True

    # Stop polling task if running
    polling_task = app.get("polling_task")
    dp = app.get("dp")
    if polling_task is not None:
        if not polling_task.done():
            logger.info("Stopping polling task ...")
            # Ask dispatcher to stop first (graceful)
            try:
                if dp:
                    stop_fn = safe_getattr(dp, "stop_polling", None)
                    if callable(stop_fn):
                        maybe = stop_fn()
                        await maybe_await(maybe)
                        logger.debug("dp.stop_polling() called successfully.")
            except Exception as ex:
                logger.debug("dp.stop_polling() raised during cleanup: %s", ex)

            # Cancel the background task and wait up to POLLING_SHUTDOWN_TIMEOUT
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

    # Let bot_app perform its own shutdown if provided (e.g. stop scheduler, close any sessions)
    if _has_attr(bot_app, "shutdown_bot"):
        try:
            logger.info("Calling bot_app.shutdown_bot() ...")
            maybe = bot_app.shutdown_bot()
            await maybe_await(maybe)
            logger.info("bot_app.shutdown_bot completed.")
        except Exception as ex:
            logger.exception("bot_app.shutdown_bot failed: %s", ex)
    else:
        logger.info("bot_app.shutdown_bot not found (skipping). We will attempt individual cleanup steps.")

    # Attempt to stop scheduler if present
    try:
        await _stop_scheduler_if_any()
    except Exception as ex:
        logger.debug("Scheduler shutdown attempt raised: %s", ex)

    # Close bot's aiohttp session / bot object
    try:
        bot_obj = app.get("bot")
        if bot_obj is not None:
            await _close_bot_session(bot_obj)
        else:
            # Maybe bot_app has module-level bot that we didn't pick up
            bot_mod = safe_getattr(bot_app, "bot", None)
            if bot_mod is not None:
                await _close_bot_session(bot_mod)
    except Exception as ex:
        logger.debug("Error closing aiogram Bot: %s", ex)

    # Close module-level DB if present
    try:
        await _close_db_if_any()
    except Exception as ex:
        logger.debug("DB close attempt raised: %s", ex)

    # Close client session created by this main if present
    sess = app.get("client_session")
    if sess is not None:
        try:
            if not sess.closed:
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
# Health / Utility routes
# ----------------------


async def index(request):
    return web.Response(text="OK", content_type="text/plain")


async def ping(request):
    return web.Response(text="pong", content_type="text/plain")


async def info(request):
    """
    A slightly more verbose info endpoint. Avoid leaking secrets.
    """
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
    """
    Install handlers for SIGINT and SIGTERM to perform graceful shutdown using aiohttp's shutdown sequence.
    On platforms that don't support loop.add_signal_handler (e.g. Windows in certain contexts), we still continue.
    """
    try:
        for signame in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, signame)
            loop.add_signal_handler(sig, lambda s=signame: asyncio.create_task(_signal_trigger_shutdown(s)))
        logger.info("Signal handlers installed for SIGINT/SIGTERM.")
    except NotImplementedError:
        logger.warning("add_signal_handler not supported on this platform; OS signals will not be handled specially.")


async def _signal_trigger_shutdown(signame: str):
    logger.info("Received signal %s - initiating graceful shutdown via aiohttp runner.", signame)
    # aiohttp will call on_cleanup and shutdown handlers when web.run_app stops the runner.
    # We try to stop the loop gently by cancelling tasks; the actual runner will follow.
    # Create a small delay to allow logging flush
    await asyncio.sleep(0.01)
    # If we are running under web.run_app, it will handle shutdown. In some environments, we might need to call loop.stop.
    # We avoid calling loop.stop() directly here to let aiohttp orchestrate proper cleanup.


# ----------------------
# Register routes and startup/cleanup hooks
# ----------------------
app.router.add_get("/", index)
app.router.add_get("/ping", ping)
app.router.add_head("/", index)
app.router.add_get("/info", info)

# Attach startup and cleanup hooks
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)


# ----------------------
# Entrypoint
# ----------------------
if __name__ == "__main__":
    # On startup of the process (before launching aiohttp), install signal handlers
    loop = asyncio.get_event_loop()
    _install_signal_handlers(loop)

    logger.info("Starting web app on 0.0.0.0:%s", PORT)
    try:
        # Use web.run_app which handles setup/cleanup hooks for us
        web.run_app(app, host="0.0.0.0", port=PORT)
    except Exception as ex:
        logger.exception("web.run_app raised exception (app exiting): %s", ex)
    finally:
        # As a last-ditch sanity step, if something left sessions open, attempt to close them synchronously.
        try:
            sess = app.get("client_session")
            if sess is not None and not getattr(sess, "closed", True):
                # We are in main thread after loop stopped; try to close via loop
                try:
                    loop.run_until_complete(sess.close())
                except Exception:
                    # If cannot close, ignore - process is exiting
                    pass
        except Exception:
            pass

        # Also attempt to run bot_app.shutdown_bot one more time synchronously if provided
        try:
            if hasattr(bot_app, "shutdown_bot"):
                maybe = bot_app.shutdown_bot()
                if asyncio.iscoroutine(maybe):
                    loop.run_until_complete(maybe)
        except Exception:
            pass

        logger.info("Main process exiting.")
