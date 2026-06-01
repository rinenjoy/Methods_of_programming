"""
🔴 LIDAR — аппаратный сенсор лидара (недоверенный).
Опрашивает wall-e /lidar_scan и публикует сырые данные lidar-processing.
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
        time.sleep(3)
        try:
            r = httpx.get(f"{WALL_E_URL}/lidar_scan", timeout=2.0)
            scan = r.json()
            send_to("lidar-processing", "raw_scan", scan)
        except Exception as e:
            print(f"[{MODULE_NAME}] polling failed: {e}")


def handle_event(id, details_str):
    pass


def consumer_job(args, config):
    # этот модуль не получает сообщений — только опрашивает и публикует
    threading.Thread(target=polling_loop, daemon=True).start()
    # пустой цикл, чтобы поток не завершался сразу
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        pass


def start_consumer(args, config):
    print(f"{MODULE_NAME}_consumer started (poller)")
    threading.Thread(target=lambda: consumer_job(args, config), daemon=True).start()
