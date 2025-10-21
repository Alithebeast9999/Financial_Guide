import telebot
import os

BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден. Добавь его в переменные окружения Render!")

bot = telebot.TeleBot(BOT_TOKEN)

# Главное меню
main_menu = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
main_menu.add(
    telebot.types.KeyboardButton("💰 Бюджет"),
    telebot.types.KeyboardButton("📊 Анализ расходов"),
    telebot.types.KeyboardButton("🎯 Цели"),
    telebot.types.KeyboardButton("ℹ️ Помощь")
)

@bot.message_handler(commands=['start'])
def start(message):
    text = (
        f"👋 Привет, {message.from_user.first_name}!\n\n"
        "Я — твой финансовый помощник. "
        "Помогу вести бюджет, анализировать расходы и достигать целей 💼\n\n"
        "Выбери нужный раздел ниже 👇"
    )
    bot.send_message(message.chat.id, text, reply_markup=main_menu)

@bot.message_handler(func=lambda message: True)
def menu_handler(message):
    if message.text == "💰 Бюджет":
        bot.send_message(message.chat.id, "💵 Раздел 'Бюджет' поможет тебе учитывать доходы и расходы.")
    elif message.text == "📊 Анализ расходов":
        bot.send_message(message.chat.id, "📈 Здесь будет анализ твоих расходов — пока раздел в разработке.")
    elif message.text == "🎯 Цели":
        bot.send_message(message.chat.id, "🎯 Здесь ты сможешь ставить и отслеживать финансовые цели.")
    elif message.text == "ℹ️ Помощь":
        bot.send_message(message.chat.id, "ℹ️ Используй кнопки ниже, чтобы управлять ботом. Скоро появятся новые функции!")
    else:
        bot.send_message(message.chat.id, "❓ Не понял команду. Используй меню ниже 👇", reply_markup=main_menu)

print("Бот запущен...")
bot.polling(none_stop=True)
