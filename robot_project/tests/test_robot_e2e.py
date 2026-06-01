import requests
import time

CONTROL_URL = 'http://localhost:8000'

MISSION_DATA = {
    "task_id": "E2E-MISSION-001",
    "token": "WALL-E-TRUSTED",
    "p_x": 25,
    "p_y": 31,
    "d_x": 12,
    "d_y": 10
}


def test_full_robot_cycle():
    
    response = requests.post(f"{CONTROL_URL}/send_mission", json=MISSION_DATA)
    assert response.status_code == 200

    start_data = response.json()

    assert "status" in start_data, f"Неожиданный ответ: {start_data}"
    assert start_data["status"] == "started"
    assert start_data["task_id"] == MISSION_DATA["task_id"]

    # Ждём старта движения
    time.sleep(2)

    max_retries = 120
    mission_completed = False

    seen_states = set()
    cargo_was_full = False
    cargo_was_empty_after = False

    for _ in range(max_retries):
        status_resp = requests.get(f"{CONTROL_URL}/robot_status")
        assert status_resp.status_code == 200

        data = status_resp.json()

        current_status = data["status"]
        pos = data["position"]
        cargo = data["cargo"]

        seen_states.add(current_status)

        # Отслеживаем цикл загрузки
        if cargo == "100%":
            cargo_was_full = True
        if cargo == "0%" and cargo_was_full:
            cargo_was_empty_after = True

        # Проверяем завершение
        if (
            current_status == "IDLE" and
            abs(pos["x"]) < 0.1 and
            abs(pos["y"]) < 0.1
        ):
            mission_completed = True
            break

        time.sleep(0.5)

    # Проверяем что прошёл все этапы
    assert "MOVING_TO_PICKUP" in seen_states
    assert "COLLECTING" in seen_states
    assert "MOVING_TO_DROPOFF" in seen_states
    assert "RETURNING_HOME" in seen_states
    assert cargo_was_full, "Робот не собрал мусор"
    assert cargo_was_empty_after, "Робот не выгрузил мусор"

    # Проверка финального состояния
    assert data["cargo"] == "0%"
    assert data["safety_lock"] == "Lid Closed"

    # Проверка позиции
    assert abs(pos["x"]) < 0.1
    assert abs(pos["y"]) < 0.1