import requests
import time
import pytest

CONTROL_URL = 'http://localhost:8000'

# Функция для проверки статуса робота
def get_robot_status():
    response = requests.get(f"{CONTROL_URL}/robot_status")
    return response.json()

# НС-7: Самопроизвольный захват (клешни) в движении
def test_ns7_claws_active_during_move():
    requests.post(f"{CONTROL_URL}/send_mission", json={
        "task_id": "CLAW_SAFETY_TEST",
        "token": "WALL-E-TRUSTED",
        "p_x": 40.0, "p_y": 40.0,
        "d_x": 0.0, "d_y": 0.0
    })

    time.sleep(2)
    
    data = get_robot_status()
    
    # Если робот в пути к мусору
    if data["status"] == "MOVING_TO_PICKUP":
        
        claws_status = data.get("claws_active", False) 
        
        print(f"Робот в статусе {data['status']}. Клешни активны: {claws_status}")
        assert claws_status == False, "КРИТИЧЕСКАЯ ОШИБКА: Клешни активны во время движения!"