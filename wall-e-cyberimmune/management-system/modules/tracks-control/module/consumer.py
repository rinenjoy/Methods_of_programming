"""
🟡 TRACKS-CONTROL — Управление гусеницами.
ЦБ1 — ограничивает скорость для безопасности (НС-9).
Передаёт команды моторам с проверкой допустимой скорости.
"""
import os
import json
import threading

from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")
MAX_SPEED = 10.0  # жёсткий предел (м/с)


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

    if src == "execution-controller" and operation == "move":
        # === ОГРАНИЧЕНИЕ СКОРОСТИ (ЦБ1) ===
        speed = min(data.get("speed", 5.0), MAX_SPEED)
        print(f"[{MODULE_NAME}] forwarding move at safe speed={speed}")
        send_to("tracks-motors", "drive", {
            "task_id": data.get("task_id"),
            "target": data.get("target"),
            "phase": data.get("phase"),
            "speed": speed,
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
