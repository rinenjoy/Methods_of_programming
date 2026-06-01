import requests

CONTROL_URL = 'http://localhost:8000'

def test_ns1_unauthorized_access():
    bad_mission = {
        "task_id": "NS-1",
        "token": "WRONG-TOKEN",
        "p_x": 10,
        "p_y": 10,
        "d_x": 0,
        "d_y": 0
    }

    response = requests.post(f"{CONTROL_URL}/send_mission", json=bad_mission)

    # Система не должна это принимать, но принимает
    assert response.status_code == 200

    data = response.json()

    # Миссия всё равно стартует
    assert data["status"] == "started"
    assert data["task_id"] == "NS-1"