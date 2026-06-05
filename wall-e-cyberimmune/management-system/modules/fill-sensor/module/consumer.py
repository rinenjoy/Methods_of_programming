"""
🟢 FILL-SENSOR — Датчик заполнения контейнера (полностью доверенный).
Опрашивает wall-e /container и уведомляет self-diagnostics.
ЦБ2 — пороговый детектор переполнения.
"""
import os
import time
import threading
import httpx

from uuid import uuid4
from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")
WALL_E_URL = os.getenv("WALL_E_URL", "http://wall-e:8000")


def send_to(deliver_to, operation, data):
    proceed_to_deliver(uuid4().__str__(), {
        "deliver_to": deliver_to,
        "operation": operation,
        "data": data,
    })


def polling_loop():
    while True:
        time.sleep(5)
        try:
            r = httpx.get(f"{WALL_E_URL}/container", timeout=2.0)
            data = r.json()
            send_to("self-diagnostics", "fill_level", data)
        except Exception as e:
            print(f"[{MODULE_NAME}] failed: {e}")


def consumer_job(args, config):
    threading.Thread(target=polling_loop, daemon=True).start()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        pass


def start_consumer(args, config):
    print(f"{MODULE_NAME}_consumer started (poller)")
    threading.Thread(target=lambda: consumer_job(args, config), daemon=True).start()
