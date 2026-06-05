"""
🟡 LIDAR-PROCESSING — Обработка информации лидара.
Фильтрация шума, построение карты препятствий → передаёт env-assessment.
"""
import os
import json
import threading

from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")


def send_to(deliver_to, operation, data):
    proceed_to_deliver(uuid4().__str__(), {
        "deliver_to": deliver_to,
        "operation": operation,
        "data": data,
    })


def filter_obstacles(points):
    """Простейшая фильтрация: оставляем точки в пределах 10 м."""
    return [p for p in (points or []) if p.get("distance", 999) < 10.0]


def handle_event(id, details_str):
    details = json.loads(details_str)
    operation = details.get("operation")
    data = details.get("data", {})

    if operation == "raw_scan":
        obstacles = filter_obstacles(data.get("points", []))
        send_to("env-assessment", "obstacle_map", {"obstacles": obstacles})


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
