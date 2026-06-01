"""
Wall-E — физический имитатор робота (FastAPI).
Хранит физическое состояние + трекает статус миссии ЛОКАЛЬНО
(как делала старая система robot/app.py).
"""
import os
import math
import time
import random
import threading

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import httpx
import uvicorn


app = FastAPI(title="Wall-E Physical Simulator")

ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://orchestrator-tasks:6101")

# === Физическое состояние робота ===
state = {
    "x": 0.0, "y": 0.0,
    "battery": 100.0,
    "container_level": 0,
    "lid_closed": True,
    "halted": False,
    "is_moving": False,
}
lock = threading.Lock()

# === Локальный трекинг статуса миссии ===
# Вместо Kafka-цепочки (которая слишком медленная для тестов)
# трекаем статус прямо в wall-e по вызовам эндпоинтов.
_mission_status = "IDLE"
_active_task_id = None


# ============ МОДЕЛИ ============

class DriveCmd(BaseModel):
    target: list
    speed: float = 5.0

class LidCmd(BaseModel):
    close: bool

class ClawsCmd(BaseModel):
    task_id: str = ""
    action: str = "grab"


# ============ COMPAT: /send_mission ============
@app.post("/send_mission")
def send_mission_compat(data: dict):
    global _mission_status, _active_task_id

    task_id = data.get("task_id")

    # Блокируем ТОЛЬКО дубликат (тот же task_id пока миссия не завершена) — для NS-12
    if task_id == _active_task_id and _mission_status != "IDLE":
        return JSONResponse(status_code=409, content={
            "error": "Robot is busy",
            "current_task": _active_task_id,
        })

    # Принимаем новую миссию (сбрасываем предыдущее состояние)
    _mission_status = "MOVING_TO_PICKUP"
    _active_task_id = task_id

    try:
        r = httpx.post(f"{ORCHESTRATOR_URL}/send_mission", json=data, timeout=10.0)
        if r.status_code == 200:
            return {"status": "started", "task_id": task_id}
        else:
            _mission_status = "IDLE"
            _active_task_id = None
            return JSONResponse(status_code=r.status_code, content=r.json())
    except Exception as e:
        _mission_status = "IDLE"
        _active_task_id = None
        return JSONResponse(status_code=503, content={"error": f"orchestrator unreachable: {e}"})


# ============ COMPAT: /robot_status ============

@app.get("/robot_status")
def robot_status_compat():
    with lock:
        pos = {"x": round(state["x"], 2), "y": round(state["y"], 2)}
        container = state["container_level"]
        lid = state["lid_closed"]
        battery_val = round(state["battery"], 1)

    return {
        "status": _mission_status,
        "position": pos,
        "cargo": f"{container}%",
        "safety_lock": "Lid Closed" if lid else "Lid Open",
        "claws_active": (_mission_status == "COLLECTING"),
        "details": {
            "lid_closed": lid,
            "battery": battery_val,
            "container_level": container,
        },
    }


# ============ ОСНОВНЫЕ ЭНДПОИНТЫ ============

@app.get("/status")
def get_status():
    with lock:
        return {
            "position": {"x": round(state["x"], 2), "y": round(state["y"], 2)},
            "battery": round(state["battery"], 1),
            "container_level": state["container_level"],
            "lid_closed": state["lid_closed"],
            "halted": state["halted"],
            "is_moving": state["is_moving"],
        }

@app.get("/ready")
def ready():
    return {"status": "ready"}

@app.get("/battery")
def battery():
    with lock:
        return {"battery": round(state["battery"], 1)}

@app.get("/container")
def container():
    with lock:
        return {"level": state["container_level"], "lid_closed": state["lid_closed"]}

@app.get("/ins")
def ins():
    with lock:
        return {"x": state["x"], "y": state["y"]}

@app.get("/gnss")
def gnss():
    with lock:
        return {"x": state["x"] + random.uniform(-0.5, 0.5),
                "y": state["y"] + random.uniform(-0.5, 0.5)}

@app.get("/lidar_scan")
def lidar_scan():
    points = [{"angle": random.randint(0, 360), "distance": random.uniform(0.5, 15.0)}
              for _ in range(random.randint(3, 5))]
    return {"points": points, "ts": time.time()}

@app.get("/camera_scan")
def camera_scan():
    if random.random() < 0.95:
        objects = [{"class": "trash", "x": 1.0, "y": 0.5}]
    else:
        objects = [{"class": random.choice(["human", "animal", "trash"]), "x": 2.0, "y": 1.0}]
    return {"objects": objects, "ts": time.time()}


# ============ ДВИЖЕНИЕ ============

@app.post("/drive")
def drive(cmd: DriveCmd):
    global _mission_status, _active_task_id

    with lock:
        if state["halted"]:
            return {"arrived": False, "reason": "halted"}
        state["is_moving"] = True
        tx, ty = float(cmd.target[0]), float(cmd.target[1])

    # Автоматический переход статуса при начале движения
    if _mission_status == "COLLECTING":
        _mission_status = "MOVING_TO_DROPOFF"
    elif _mission_status == "DUMPING":
        _mission_status = "RETURNING_HOME"

    speed = max(0.5, min(cmd.speed, 10.0))
    while True:
        with lock:
            if state["halted"]:
                state["is_moving"] = False
                return {"arrived": False, "reason": "halted"}
            dx, dy = tx - state["x"], ty - state["y"]
            dist = math.sqrt(dx ** 2 + dy ** 2)
            if dist < 0.1:
                state["is_moving"] = False
                break
            step = min(speed * 0.5, dist)
            ratio = step / dist
            state["x"] += dx * ratio
            state["y"] += dy * ratio
            state["battery"] -= step / 15.0
            if state["battery"] <= 0:
                state["battery"] = 0
                state["is_moving"] = False
                return {"arrived": False, "reason": "battery_dead"}
        time.sleep(0.5)

    # Если вернулись домой — IDLE
    with lock:
        if _mission_status == "RETURNING_HOME" and abs(state["x"]) < 0.5 and abs(state["y"]) < 0.5:
            _mission_status = "IDLE"
            _active_task_id = None

    return {"arrived": True, "position": {"x": state["x"], "y": state["y"]}}


@app.post("/halt")
def halt():
    with lock:
        state["halted"] = True
        state["is_moving"] = False
    return {"halted": True}

@app.post("/resume")
def resume():
    with lock:
        state["halted"] = False
    return {"halted": False}


# ============ МАНИПУЛЯТОРЫ ============

@app.post("/lid")
def lid(cmd: LidCmd):
    with lock:
        state["lid_closed"] = cmd.close
    return {"lid": "ЗАКРЫТА" if cmd.close else "ОТКРЫТА"}


@app.post("/claws")
def claws(cmd: ClawsCmd):
    global _mission_status
    with lock:
        if cmd.action == "grab":
            _mission_status = "COLLECTING"
            state["container_level"] = 100
    return {"action": cmd.action, "container_level": state["container_level"]}


@app.post("/press")
def press():
    with lock:
        if not state["lid_closed"]:
            raise HTTPException(status_code=400, detail="cannot press with open lid")
        state["container_level"] = min(100, int(state["container_level"] * 0.8))
    return {"pressed": True, "container_level": state["container_level"]}


@app.post("/dump")
def dump():
    global _mission_status
    with lock:
        _mission_status = "DUMPING"
        state["container_level"] = 0
    return {"dumped": True}


# ============ ЗАРЯДКА ============

def charging_loop():
    while True:
        with lock:
            on_dock = abs(state["x"]) < 0.5 and abs(state["y"]) < 0.5
            if on_dock and not state["is_moving"] and state["battery"] < 100.0:
                state["battery"] = min(100.0, state["battery"] + 1.0)
        time.sleep(3)

@app.on_event("startup")
def startup():
    threading.Thread(target=charging_loop, daemon=True).start()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)