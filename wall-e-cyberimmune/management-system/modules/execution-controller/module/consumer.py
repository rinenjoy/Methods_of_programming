"""
🔴 EXECUTION-CONTROLLER — Исполнительный контроллер (недоверенный).
Получает безопасную траекторию от motion-planner и передаёт команды tracks-control.
Под наблюдением emergency-block через emergency-stop.
"""
import os
import json
import threading

from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")
_last_position = {"x": 0.0, "y": 0.0}


def send_to(deliver_to, operation, data):
    proceed_to_deliver(uuid4().__str__(), {
        "deliver_to": deliver_to,
        "operation": operation,
        "data": data,
    })


def handle_event(id, details_str):
    details = json.loads(details_str)
    src = details.get("source")
    operation = details.get("operation")
    data = details.get("data", {})

    if src == "nav-fusion" and operation == "position":
        _last_position["x"] = data.get("x", 0.0)
        _last_position["y"] = data.get("y", 0.0)
        return

    if src == "motion-planner" and operation == "execute_trajectory":
        target = data.get("target")
        print(f"[{MODULE_NAME}] executing trajectory to {target}")
        # передаём команду на гусеницы
        send_to("tracks-control", "move", {
            "task_id": data.get("task_id"),
            "target": target,
            "phase": data.get("phase"),
            "speed": 5.0,
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
