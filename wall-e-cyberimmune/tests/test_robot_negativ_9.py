import requests
import time

CONTROL_URL = 'http://localhost:8000'


def get_robot_status():
    return requests.get(f"{CONTROL_URL}/robot_status").json()


def test_ns9_track_failure_not_detected():
    requests.post(f"{CONTROL_URL}/send_mission", json={
        "task_id": "NS-9",
        "token": "WALL-E-TRUSTED",
        "p_x": 50,
        "p_y": 50,
        "d_x": 0,
        "d_y": 0
    })

    moving_detected = False

    for _ in range(20):
        status = get_robot_status()
        current_status = status.get("status")

        if current_status in ["MOVING_TO_PICKUP", "MOVING_TO_DROPOFF"]:
            moving_detected = True

        time.sleep(1)

    # Робот продолжает ехать, даже если "сломался"
    assert moving_detected, "Робот не двигался"
