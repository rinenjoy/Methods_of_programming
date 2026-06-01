import requests
import time

CONTROL_URL = 'http://localhost:8000'


def get_robot_status():
    return requests.get(f"{CONTROL_URL}/robot_status").json()


def test_ns5_low_battery_after_mission():
    # Первая миссия
    requests.post(f"{CONTROL_URL}/send_mission", json={
        "task_id": "DRAIN",
        "token": "WALL-E-TRUSTED",
        "p_x": 300,
        "p_y": 300,
        "d_x": 0,
        "d_y": 0
    })

    time.sleep(80)

    response = requests.post(f"{CONTROL_URL}/send_mission", json={
        "task_id": "LOW_BATTERY",
        "token": "WALL-E-TRUSTED",
        "p_x": 10,
        "p_y": 10,
        "d_x": 0,
        "d_y": 0
    })

    data = response.json()
    print("Second mission response:", data)

    # Робот завис и не принимает новую задачу
    if "error" in data:
        assert data["error"] == "Robot is busy"

    # Робот принял задачу при низкой батарее
    elif "status" in data:
        assert data["status"] == "started"

        time.sleep(2)
        status = get_robot_status()["status"]

        assert status in [
            "MOVING_TO_PICKUP",
            "COLLECTING",
            "MOVING_TO_DROPOFF"
        ]

    else:
        assert False, f"Вообще непонятно что вернулось: {data}"