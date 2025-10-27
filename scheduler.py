
import asyncio
import logging

logger = logging.getLogger(__name__)

async def start_scheduler(app):
    async def daily_task():
        while True:
            logger.info("Scheduler tick (reminders, recurring, reports...)")
            await asyncio.sleep(86400)  # раз в день

    app.create_task(daily_task())
