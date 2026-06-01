import os
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Модель данных для входящего запроса
# Оператор передает ID задачи, секретный токен и две точки координат
class Mission(BaseModel):
    task_id: str
    token: str
    p_x: float  # Точка сбора (pickup) X
    p_y: float  # Точка сбора (pickup) Y
    d_x: float  # Точка выгрузки (dropoff) X
    d_y: float  # Точка выгрузки (dropoff) Y

app = FastAPI(title="Wall-E Control Center")

# URL робота
ROBOT_URL = os.getenv('ROBOT_URL', 'http://robot:8001')

@app.post("/send_mission")
async def send_mission(mission: Mission):
    """
    Основной эндпоинт для отправки Валли на задание.
    Здесь происходит проверка безопасности и запуск цикла.
    """
    

    print(f"Задача {mission.task_id} принята. Валли отправляется на цикл.")

    # Готовим данные для передачи самому роботу
    # Объединяем координаты в понятный формат
    payload = {
        "task_id": mission.task_id,
        "pickup": [mission.p_x, mission.p_y],
        "dropoff": [mission.d_x, mission.d_y]
    }

    # Отправляем команду роботу через HTTP-запрос
    async with httpx.AsyncClient() as client:
        try:
            # Робот должен начать выполнение цикла
            response = await client.post(f"{ROBOT_URL}/start_cycle", json=payload)
            return response.json()
        except Exception as e:
            print(f"Ошибка: Робот не отвечает. {e}")
            raise HTTPException(status_code=503, detail="Робот недоступен")

@app.get("/robot_status")
async def get_robot_status():
    """
    Узнать, где сейчас Валли и чем он занят
    """
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f"{ROBOT_URL}/status")
            return r.json()
        except:
            return {"status": "offline", "message": "Связь с Валли потеряна"}