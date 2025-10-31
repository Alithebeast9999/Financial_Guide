# Используем стабильный Python 3.11
FROM python:3.11-slim

WORKDIR /app

# Копируем файлы проекта
COPY . .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Открываем порт
EXPOSE 10000

# Запуск приложения
CMD ["python", "main.py"]
