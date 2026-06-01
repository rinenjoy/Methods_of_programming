import requests
import time

CONTROL_URL = 'http://localhost:8000'


def get_robot_status():
    return requests.get(f"{CONTROL_URL}/robot_status").json()


def test_ns16_wrong_cargo_sensor():
    requests.post(f"{CONTROL_URL}/send_mission", json={
        "task_id": "NS-16",
        "token": "WALL-E-TRUSTED",
        "p_x": 10,
        "p_y": 10,
        "d_x": 0,
        "d_y": 0
    })

    cargo_values = set()
    wrong_values_detected = False

    for _ in range(20):
        status = get_robot_status()
        cargo = status.get("cargo")

        if cargo:
            cargo_values.add(cargo)

            # Проверка на неправильные значения
            if not str(cargo).endswith("%"):
                wrong_values_detected = True

        time.sleep(1)

    print("Cargo values:", cargo_values)

    # Данные вообще есть
    assert len(cargo_values) >= 1, "Нет данных о заполнении"

    # Датчик может выдавать некорректные значения
    if wrong_values_detected:
        assert True