import telebot
import os

TOKEN = os.getenv("BOT_TOKEN")
bot = telebot.TeleBot(TOKEN)

@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "👋 Привет! Я Financial Guide — помогу разобраться с финансами.")

@bot.message_handler(func=lambda m: True)
def echo_all(message):
    bot.send_message(message.chat.id, "💡 Напиши /start, чтобы начать.")

if __name__ == "__main__":
    bot.polling(none_stop=True)
