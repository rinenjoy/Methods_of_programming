"""
🟡 HYDRAULIC-PRESS — Гидравлический пресс.
ЦБ1/ЦБ2 — не активирует сжатие при открытой крышке (НС-11).
"""
import os
import json
import threading
import httpx

from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")
WALL_E_URL = os.getenv("WALL_E_URL", "http://wall-e:8000")


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

    if operation == "press":
        # === ПРОВЕРКА БЕЗОПАСНОСТИ (ЦБ2) ===
        try:
            r = httpx.get(f"{WALL_E_URL}/status", timeout=3.0)
            status = r.json()
            if not status.get("lid_closed", True):
                print(f"[{MODULE_NAME}] BLOCKED press — lid is OPEN!")
                return
        except Exception as e:
            print(f"[error] status check: {e}")
            return

        send_to("hydraulic-control", "compress", data)


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
