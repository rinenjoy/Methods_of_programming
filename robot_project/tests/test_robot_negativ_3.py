import requests
import time

CONTROL_URL = 'http://localhost:8000'

def get_robot_status():
    response = requests.get(f"{CONTROL_URL}/robot_status")
    return response.json()


def test_ns3_out_of_bounds_mission():
    huge_coords_mission = {
        "task_id": "NS-3",
        "token": "WALL-E-TRUSTED",
        "p_x": 120.0, 
        "p_y": 120.0,
        "d_x": 0,
        "d_y": 0
    }

    response = requests.post(f"{CONTROL_URL}/send_mission", json=huge_coords_mission)

    # Система принимает некорректные координаты
    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "started"

    # Даём роботу начать движение
    time.sleep(2)

    status = get_robot_status()

    # Робот всё равно едет к точке вне зоны
    assert status["status"] in [
        "MOVING_TO_PICKUP",
        "COLLECTING",
        "MOVING_TO_DROPOFF"
    ]