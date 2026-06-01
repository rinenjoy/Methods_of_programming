# Используем легкий образ Python
FROM python:3.11-slim

# Указываем рабочую папку внутри контейнера
WORKDIR /app

# Сначала копируем только зависимости (это ускоряет сборку)
COPY requirements.txt .

# Устанавливаем библиотеки:
# fastapi и uvicorn для сервера, httpx для запросов к роботу
RUN pip install --no-cache-dir fastapi uvicorn httpx pydantic

# Копируем наш код в контейнер
COPY app.py .

# Валли слушает на порту 8000
EXPOSE 8000

# Команда для запуска сервера:
# --host 0.0.0.0 нужен, чтобы контейнер был виден снаружи
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]