"""
🟡 SELF-DIAGNOSTICS — Самодиагностика робота.
Агрегирует состояние подсистем и уведомляет emergency-block при проблемах.
"""
import os
import json
import threading

from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")

_state = {"fill": 0, "lid_closed": True, "battery": 100}
FILL_OVERFLOW = 90


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

    if operation == "fill_level":
        level = data.get("level", 0)
        _state["fill"] = level
        if level >= FILL_OVERFLOW:
            send_to("emergency-block", "status", {"container_overflow": True, "level": level})


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
