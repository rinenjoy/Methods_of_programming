import requests
import time
import pytest

CONTROL_URL = 'http://localhost:8000'

# Функция для проверки статуса робота
def get_robot_status():
    response = requests.get(f"{CONTROL_URL}/robot_status")
    return response.json()

# НС-10: Открытие крышки в пути
def test_ns10_lid_opens_on_route():
    requests.post(f"{CONTROL_URL}/send_mission", json={
        "task_id": "LID_TEST", "token": "WALL-E-TRUSTED",
        "p_x": 30, "p_y": 30, "d_x": 0, "d_y": 0
    })
    
    time.sleep(1.5)
    status = requests.get(f"{CONTROL_URL}/robot_status").json()
    
    if "MOVING" in status["status"]:
        assert status["safety_lock"] == "Lid Closed"
        print("Крышка в движении закрыта!")