import requests
import time

CONTROL_URL = 'http://localhost:8000'

def get_robot_status():
    response = requests.get(f"{CONTROL_URL}/robot_status")
    return response.json()


def test_ns4_lidar_not_working():
    LIDAR_FAIL = {
        "task_id": "NS-4",
        "token": "WALL-E-TRUSTED",
        "p_x": 10,
        "p_y": 10,
        "d_x": 5,
        "d_y": 5
        }

    response = requests.post(f"{CONTROL_URL}/send_mission", json=LIDAR_FAIL)
    
    seen_states = set()

    timeout = 15
    start_time = time.time()

    while time.time() - start_time < timeout:
        status = get_robot_status()["status"]
        seen_states.add(status)

        time.sleep(0.5)

    # Робот проходит все стадии, как будто препятствий нет
    assert "MOVING_TO_PICKUP" in seen_states
    assert "COLLECTING" in seen_states
    assert "MOVING_TO_DROPOFF" in seen_states
    assert "RETURNING_HOME" in seen_states or "IDLE" in seen_states