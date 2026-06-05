"""
🟡 TRASH-DUMP — Сложить мусор.
ЦБ2 — выгрузка только в авторизованной зоне (НС-2).
Зона разгрузки = вся авторизованная территория (-50..50).
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

# Авторизованная зона разгрузки = вся рабочая территория
ZONE_MIN, ZONE_MAX = -50.0, 50.0


def in_authorized_zone(x, y):
    return ZONE_MIN <= x <= ZONE_MAX and ZONE_MIN <= y <= ZONE_MAX


def send_to(deliver_to, operation, data):
    proceed_to_deliver(uuid4().__str__(), {
        "deliver_to": deliver_to,
        "operation": operation,
        "data": data,
    })


def handle_event(id, details_str):
    details = json.loads(details_str)
    operation = details.get("operation")
    data = details.get("data", {})

    if operation == "dump":
        task_id = data.get("task_id")
        try:
            r = httpx.get(f"{WALL_E_URL}/status", timeout=3.0)
            status = r.json()
            x, y = status["position"]["x"], status["position"]["y"]
            if not in_authorized_zone(x, y):
                print(f"[{MODULE_NAME}] BLOCKED dump at ({x},{y}) — outside authorized zone!")
                return
            send_to("lid-system", "open_lid", {})
            time.sleep(0.2)
            r = httpx.post(f"{WALL_E_URL}/dump", timeout=5.0)
            print(f"[{MODULE_NAME}] wall-e dump: {r.json()}")
            time.sleep(0.2)
            send_to("lid-system", "close_lid", {})
            time.sleep(0.2)
            send_to("task-handler", "stage_done", {"task_id": task_id, "stage": "dumping"})
        except Exception as e:
            print(f"[error] dump: {e}")


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