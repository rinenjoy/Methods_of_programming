import requests
import time
import pytest

CONTROL_URL = 'http://localhost:8000'

# Функция для проверки статуса робота
def get_robot_status():
    response = requests.get(f"{CONTROL_URL}/robot_status")
    return response.json()
# НС-12: Дублирование задания
def test_ns12_duplicate_task_id():
    mission = {"task_id": "DUP-1", "token": "WALL-E-TRUSTED", "p_x": 1, "p_y": 1, "d_x": 0, "d_y": 0}
    requests.post(f"{CONTROL_URL}/send_mission", json=mission)
    # Повторная отправка того же ID
    response = requests.post(f"{CONTROL_URL}/send_mission", json=mission)
    assert response.status_code != 200 or "error" in response.json()

