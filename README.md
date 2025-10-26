# Finance Assistant Telegram Bot

Simple Telegram bot (webhook) that helps users manage monthly budgets, category limits, add expenses,
recurring expenses, daily reminders and weekly reports.

## Quick start

1. Fill .env or set environment variables:
   - BOT_TOKEN
   - WEBHOOK_URL (e.g. https://<your-service>.onrender.com/webhook)
   - PORT (Render provides this automatically)

2. Deploy to Render (or any web host). Example `render.yaml` included.

3. Commands:
   - /start
   - /setbudget <amount>
   - /showlimits
   - /addexpense
   - /addrecurring
   - /report week|month
   - /history
   - /notifications on|off

DB: SQLite `data.sqlite` by default. For persistence across deploys use external DB (Postgres) and set DATABASE_URL.

