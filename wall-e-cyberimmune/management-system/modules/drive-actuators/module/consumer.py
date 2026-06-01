"""
🟢 DRIVE-ACTUATORS — Приводы движения (доверенные).
Конечная точка движения: HTTP-запрос к физическому имитатору wall-e.
Также принимает команду halt от emergency-stop (ЦБ1).
"""
import os
import json
import time
import threading

import httpx
from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")
WALL_E_URL = os.getenv("WALL_E_URL", "http://wall-e:8000")

_halted = False


def send_to(deliver_to, operation, data):
    proceed_to_deliver(uuid4().__str__(), {
        "deliver_to": deliver_to,
        "operation": operation,
        "data": data,
    })


def handle_event(id, details_str):
    global _halted
    details = json.loads(details_str)
    src = details.get("source")
    operation = details.get("operation")
    data = details.get("data", {})

    if src == "emergency-stop" and operation == "halt":
        print(f"[{MODULE_NAME}] !!! EMERGENCY HALT received !!!")
        _halted = True
        try:
            httpx.post(f"{WALL_E_URL}/halt", timeout=2.0)
        except Exception as e:
            print(f"[error] halt failed: {e}")
        return

    if operation == "rotate":
        if _halted:
            print(f"[{MODULE_NAME}] ignoring rotate — halted")
            return
        target = data.get("target")
        speed = data.get("speed", 5.0)
        phase = data.get("phase")
        task_id = data.get("task_id")
        try:
            r = httpx.post(
                f"{WALL_E_URL}/drive",
                json={"target": target, "speed": speed},
                timeout=120.0,
            )
            result = r.json()
            print(f"[{MODULE_NAME}] wall-e {result}")
            # Уведомляем task-handler о прибытии на этап (требует политику drive-actuators → task-handler)
            if result.get("arrived"):
                send_to("task-handler", "stage_arrived", {
                    "task_id": task_id,
                    "phase": phase,
                    "position": result.get("position"),
                })
        except Exception as e:
            print(f"[error] drive failed: {e}")


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
                handle_event(msg.key().decode("utf-8"), msg.value().decode("utf-8"))
            except Exception as e:
                print(f"[error] {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def start_consumer(args, config):
    print(f"{MODULE_NAME}_consumer started")
    threading.Thread(target=lambda: consumer_job(args, config), daemon=True).start()
