import requests
import time
import pytest

CONTROL_URL = 'http://localhost:8000'

# Функция для проверки статуса робота
def get_robot_status():
    response = requests.get(f"{CONTROL_URL}/robot_status")
    return response.json()
# НС-11: Сжатие при открытой крышке
def test_ns11_compression_safety():
    requests.post(f"{CONTROL_URL}/send_mission", json={
        "task_id": "LID_LOGIC_TEST",
        "token": "WALL-E-TRUSTED",
        "p_x": 10.0, "p_y": 10.0,
        "d_x": 20.0, "d_y": 20.0
    })

    for _ in range(20):
        data = get_robot_status()
        current_status = data["status"]
        lock = data["safety_lock"]

        if current_status not in ["COLLECTING", "DUMPING"]:
            print(f"Статус: {current_status} | Крышка: {lock} — OK")
            assert lock == "Lid Closed", f"ОШИБКА: Крышка открыта в статусе {current_status}!"
        
        time.sleep(0.5)