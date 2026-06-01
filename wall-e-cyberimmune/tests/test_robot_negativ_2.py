import requests
import time

CONTROL_URL = 'http://localhost:8000'


def get_robot_status():
    return requests.get(f"{CONTROL_URL}/robot_status").json()


def test_ns2_dump_outside_zone():
    # Отправляем миссию
    requests.post(f"{CONTROL_URL}/send_mission", json={
        "task_id": "NS-2",
        "token": "WALL-E-TRUSTED",
        "p_x": 5,
        "p_y": 5,
        "d_x": 20,  # зона выгрузки где-то далеко
        "d_y": 20
    })

    dumping_detected = False

    # Наблюдаем
    for _ in range(20):
        status = get_robot_status()
        current_status = status.get("status")

        if current_status == "DUMPING":
            dumping_detected = True

        time.sleep(1)

    # Робот может выгружаться где угодно
    assert dumping_detected, "Робот не начал выгрузку"
