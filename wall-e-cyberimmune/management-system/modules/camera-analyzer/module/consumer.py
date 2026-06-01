"""
🔴 CAMERA-ANALYZER — Камера + ML-классификатор объектов (недоверенный).
Опрашивает wall-e /camera_scan и публикует найденные объекты в env-assessment.
"""
import os
import time
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


def polling_loop():
    while True:
        time.sleep(4)
        try:
            r = httpx.get(f"{WALL_E_URL}/camera_scan", timeout=2.0)
            scan = r.json()
            send_to("env-assessment", "objects", {"objects": scan.get("objects", [])})
        except Exception as e:
            print(f"[{MODULE_NAME}] polling failed: {e}")


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
