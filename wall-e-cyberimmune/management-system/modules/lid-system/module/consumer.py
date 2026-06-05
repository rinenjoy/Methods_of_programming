"""
🔴 LID-SYSTEM — Система управления крышкой (недоверенная).
HTTP-запрос к wall-e для открытия/закрытия физической крышки.
"""
import os
import json
import threading
import httpx

from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")
WALL_E_URL = os.getenv("WALL_E_URL", "http://wall-e:8000")


def handle_event(id, details_str):
    details = json.loads(details_str)
    operation = details.get("operation")

    if operation in ("open_lid", "close_lid"):
        close = (operation == "close_lid")
        try:
            r = httpx.post(f"{WALL_E_URL}/lid", json={"close": close}, timeout=5.0)
            print(f"[{MODULE_NAME}] wall-e lid: {r.json()}")
        except Exception as e:
            print(f"[error] lid: {e}")


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
