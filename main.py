
import os
import logging
from telegram.ext import Application
from db import init_db
from scheduler import start_scheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 8080))

def build_app():
    from handlers import register_handlers
    app = Application.builder().token(os.getenv("BOT_TOKEN")).build()
    register_handlers(app)
    return app

def main():
    init_db()
    app = build_app()

    async def _on_startup(app):
        if WEBHOOK_URL:
            try:
                await app.bot.set_webhook(WEBHOOK_URL)
                logger.info("Webhook set: %s", WEBHOOK_URL)
            except Exception:
                logger.exception("set_webhook failed")
        await start_scheduler(app)

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="/webhook",
        webhook_url=WEBHOOK_URL,
        post_init=_on_startup,
    )

if __name__ == "__main__":
    main()
