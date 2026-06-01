import requests
import time

CONTROL_URL = 'http://localhost:8000'


def get_robot_status():
    return requests.get(f"{CONTROL_URL}/robot_status").json()


def test_ns13_object_classification_error():
    # Отправляем миссию
    response = requests.post(f"{CONTROL_URL}/send_mission", json={
        "task_id": "NS-13",
        "token": "WALL-E-TRUSTED",
        "p_x": 10,
        "p_y": 10,
        "d_x": 0,
        "d_y": 0
    })

    assert response.status_code == 200

    collecting_detected = False

    # Наблюдаем за роботом
    for _ in range(15):
        status = get_robot_status()
        current_status = status.get("status")

        if current_status == "COLLECTING":
            collecting_detected = True

        time.sleep(1)

    # Робот начинает сбор без проверки объекта
    assert collecting_detected, "Робот не начал сбор"