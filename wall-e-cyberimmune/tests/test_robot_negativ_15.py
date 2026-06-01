import requests
import time

CONTROL_URL = 'http://localhost:8000'


def get_robot_status():
    return requests.get(f"{CONTROL_URL}/robot_status").json()

# Управление передвижением не успевает пересчитать маршрут при внезапном препятствии
def test_ns15_no_reroute_on_obstacle():
    requests.post(f"{CONTROL_URL}/send_mission", json={
        "task_id": "NS-15",
        "token": "WALL-E-TRUSTED",
        "p_x": 30,
        "p_y": 30,
        "d_x": 0,
        "d_y": 0
    })

    moving_detected = False

    for _ in range(20):
        status = get_robot_status()
        current_status = status.get("status")

        if current_status == "MOVING_TO_PICKUP":
            moving_detected = True

        time.sleep(1)

    # Робот просто едет и не реагирует на препятствия
    assert moving_detected, "Робот не двигался"
