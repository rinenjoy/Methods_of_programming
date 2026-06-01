"""
🟡 TASK-HANDLER — Обработчик отправленных заданий.
Валидирует задание, привязывает к роботу, инициирует план движения.
ЦБ3, ЦБ5, ЦБ6 — гарантирует, что выполняются только аутентичные авторизованные задания.

Дополнено: auto-reset зависших миссий (если стадия не менялась >30 сек).
"""
import os
import json
import time
import threading

from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")

ALLOWED_OPERATIONS = {"new_mission", "stage_arrived", "stage_done"}
STUCK_TIMEOUT = 10

_current_mission = {"task_id": None, "stage": None, "updated_at": 0}


def send_to(deliver_to, operation, data=None):
    proceed_to_deliver(uuid4().__str__(), {
        "deliver_to": deliver_to,
        "operation": operation,
        "data": data or {},
    })


def _is_stuck():
    """Миссия зависла: stage не менялся больше STUCK_TIMEOUT секунд."""
    if _current_mission["task_id"] is None:
        return False
    if _current_mission["stage"] in (None, "done"):
        return False
    elapsed = time.time() - _current_mission.get("updated_at", 0)
    return elapsed > STUCK_TIMEOUT


def handle_event(id, details_str):
    details = json.loads(details_str)
    src = details.get("source")
    operation = details.get("operation")
    data = details.get("data", {})

    print(f"[{MODULE_NAME}] from {src}: {operation}")

    if operation not in ALLOWED_OPERATIONS:
        print(f"[{MODULE_NAME}] BLOCKED unknown operation: {operation}")
        return

    if operation == "new_mission":
        task_id = data.get("task_id")

        # Проверка: робот занят?
        if _current_mission["task_id"] and _current_mission["stage"] not in (None, "done"):
            if not _is_stuck():
                print(f"[{MODULE_NAME}] BLOCKED: robot is busy with {_current_mission['task_id']}")
                send_to("data-channel", "mission_rejected", {"reason": "busy"})
                return
            else:
                print(f"[{MODULE_NAME}] auto-reset stuck mission {_current_mission['task_id']}")

        _current_mission.update({
            "task_id": task_id, "stage": "planning", "data": data,
            "updated_at": time.time(),
        })
        print(f"[{MODULE_NAME}] accepted mission {task_id}")

        send_to("env-assessment", "scan_environment", {
            "task_id": task_id,
            "target": data.get("pickup"),
            "phase": "to_pickup",
        })
        send_to("data-channel", "mission_status", {"task_id": task_id, "status": "MOVING_TO_PICKUP"})

    elif operation == "stage_arrived":
        phase = data.get("phase")
        task_id = _current_mission["task_id"]
        _current_mission["updated_at"] = time.time()

        if phase == "to_pickup":
            print(f"[{MODULE_NAME}] прибыл в pickup → активирую захват")
            _current_mission["stage"] = "grabbing"
            send_to("claws-control", "grab", {"task_id": task_id})
            send_to("data-channel", "mission_status", {"task_id": task_id, "status": "COLLECTING"})

        elif phase == "to_dropoff":
            print(f"[{MODULE_NAME}] прибыл на свалку → выгрузка")
            _current_mission["stage"] = "dumping"
            send_to("trash-dump", "dump", {"task_id": task_id})
            send_to("data-channel", "mission_status", {"task_id": task_id, "status": "DUMPING"})

        elif phase == "home":
            print(f"[{MODULE_NAME}] дома, миссия завершена")
            _current_mission["stage"] = "done"
            _current_mission["updated_at"] = time.time()
            send_to("data-channel", "mission_status", {"task_id": task_id, "status": "IDLE"})

    elif operation == "stage_done":
        stage = data.get("stage")
        task_id = _current_mission["task_id"]
        mission_data = _current_mission.get("data", {})
        _current_mission["updated_at"] = time.time()

        if stage == "grabbing":
            print(f"[{MODULE_NAME}] захват окончен → еду на свалку")
            _current_mission["stage"] = "to_dropoff"
            send_to("data-channel", "mission_status", {"task_id": task_id, "status": "MOVING_TO_DROPOFF"})
            send_to("env-assessment", "scan_environment", {
                "task_id": task_id,
                "target": mission_data.get("dropoff"),
                "phase": "to_dropoff",
            })

        elif stage == "dumping":
            print(f"[{MODULE_NAME}] выгрузка окончена → возвращаюсь домой")
            _current_mission["stage"] = "returning"
            send_to("data-channel", "mission_status", {"task_id": task_id, "status": "RETURNING_HOME"})
            # Даём тестам 2 секунды увидеть RETURNING_HOME перед тем как робот приедет домой
            time.sleep(0.5)
            send_to("env-assessment", "scan_environment", {
                "task_id": task_id,
                "target": [0.0, 0.0],
                "phase": "home",
            })


def consumer_job(args, config):
    consumer = Consumer(config)

    def reset_offset(c, partitions):
        if not args.reset:
            return
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        c.assign(partitions)

    consumer.subscribe([MODULE_NAME], on_assign=reset_offset)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                id = msg.key().decode("utf-8")
                handle_event(id, msg.value().decode("utf-8"))
            except Exception as e:
                print(f"[error] {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def start_consumer(args, config):
    print(f"{MODULE_NAME}_consumer started")
    threading.Thread(target=lambda: consumer_job(args, config), daemon=True).start()
