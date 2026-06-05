"""
🟡 ORCHESTRATOR-TASKS — точка входа оператора (FastAPI).
Принимает POST /send_mission, валидирует, кладёт в Kafka на data-channel.
GET /robot_status возвращает последний известный статус.
"""
import os
import time
import json
import threading
import multiprocessing

from uuid import uuid4
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn


HOST = "0.0.0.0"
PORT = int(os.getenv("MODULE_PORT", "6101"))
MODULE_NAME = os.getenv("MODULE_NAME", "orchestrator-tasks")
MAX_WAIT_TIME = 30

_requests_queue: multiprocessing.Queue = None
_response_queue: multiprocessing.Queue = None

# простой кэш статусов
_last_status = {"status": "IDLE", "info": "Робот ожидает заданий"}


app = FastAPI(title="Wall-E Orchestrator")


class Mission(BaseModel):
    task_id: str
    token: str
    p_x: float
    p_y: float
    d_x: float
    d_y: float


def send_to_data_channel(operation: str, data: dict):
    details = {
        "id": uuid4().__str__(),
        "source": MODULE_NAME,
        "deliver_to": "data-channel",
        "operation": operation,
        "data": data,
    }
    _requests_queue.put(details)
    print(f"[{MODULE_NAME}] → data-channel: {operation}")


def wait_response(timeout=MAX_WAIT_TIME):
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            return None
        try:
            response = _response_queue.get(timeout=2.0)
        except Exception:
            continue
        if not isinstance(response, dict):
            continue
        if response.get("deliver_to") != MODULE_NAME:
            continue
        return response.get("data")


@app.post("/send_mission")
async def send_mission(mission: Mission):
    """Принять задание от оператора и отправить роботу."""
    # === ВАЛИДАЦИЯ (часть доверенной логики, ЦБ4/ЦБ5) ===
    if not mission.token or len(mission.token) < 8:
        raise HTTPException(status_code=401, detail="Невалидный токен оператора")
    if not mission.task_id:
        raise HTTPException(status_code=400, detail="task_id обязателен")

    payload = {
        "task_id": mission.task_id,
        "token": mission.token,
        "pickup": [mission.p_x, mission.p_y],
        "dropoff": [mission.d_x, mission.d_y],
    }
    send_to_data_channel("send_mission", payload)
    return {"status": "accepted", "task_id": mission.task_id}


@app.get("/robot_status")
async def get_status():
    return _last_status


@app.get("/ready")
async def ready():
    return {"status": "ready"}


def update_status(new_status: dict):
    global _last_status
    _last_status.update(new_status)


def start_web(requests_queue, response_queue):
    global _requests_queue, _response_queue
    _requests_queue = requests_queue
    _response_queue = response_queue

    threading.Thread(
        target=lambda: uvicorn.run(app, host=HOST, port=PORT, log_level="info"),
        daemon=True,
    ).start()
