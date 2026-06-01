import requests
import time

CONTROL_URL = 'http://localhost:8000'


def get_robot_status():
    return requests.get(f"{CONTROL_URL}/robot_status").json()


def test_ns6_malfunction_exit():
    status_data = get_robot_status()
    details = status_data.get("details", {})

    # Если система уже с открытой крышкой
    if not details.get("lid_closed", True):
        response = requests.post(f"{CONTROL_URL}/send_mission", json={
            "task_id": "LID_OPEN",
            "token": "WALL-E-TRUSTED",
            "p_x": 10,
            "p_y": 10,
            "d_x": 0,
            "d_y": 0
        })

        data = response.json()

        assert response.status_code == 200

        if "status" in data:
            assert data["status"] == "started"

            time.sleep(2)
            status = get_robot_status()["status"]

            # Робот поехал с открытой крышкой
            assert status in [
                "MOVING_TO_PICKUP",
                "COLLECTING",
                "MOVING_TO_DROPOFF"
            ]
    else:
        print("Крышка закрыта — система работает в нормальном режиме")