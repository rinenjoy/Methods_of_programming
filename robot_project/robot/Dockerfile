# Используем легкий Python
FROM python:3.11-slim

WORKDIR /app

# Нам нужны fastapi и uvicorn (сервер), а также pydantic (проверка данных)
RUN pip install --no-cache-dir fastapi uvicorn pydantic

# Копируем код
COPY app.py .

# Робот живет на порту 8001
EXPOSE 8001

# Запуск сервера
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]