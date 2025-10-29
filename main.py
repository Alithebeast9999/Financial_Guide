async def main():
    WEBHOOK_URL = os.getenv("RENDER_EXTERNAL_URL")
    await app.bot.delete_webhook()
    await asyncio.sleep(1)
    await app.bot.set_webhook(f"{WEBHOOK_URL}/webhook")

    await app.run_webhook(
        listen="0.0.0.0",
        port=int(os.environ.get("PORT", 8080)),
        webhook_url=f"{WEBHOOK_URL}/webhook",
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    import asyncio

    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "already running" in str(e):
            loop = asyncio.get_event_loop()
            loop.create_task(main())
            loop.run_forever()
        else:
            raise e
