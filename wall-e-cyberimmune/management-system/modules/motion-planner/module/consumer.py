"""
🟢 MOTION-PLANNER — Планировщик движения (полностью доверенный).
ЦБ1 — гарантирует, что маршрут проходит в авторизованной зоне и без препятствий.
Получает от env-assessment безопасную карту, от nav-fusion — текущую позицию.
Отдает execution-controller безопасную траекторию.
"""
import os
import json
import threading

from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")

# Авторизованная зона работы (ЦБ1)
ZONE_MIN_X = -50.0
ZONE_MIN_Y = -50.0
ZONE_MAX_X = 50.0
ZONE_MAX_Y = 50.0

_last_position = {"x": 0.0, "y": 0.0}


def send_to(deliver_to, operation, data):
    proceed_to_deliver(uuid4().__str__(), {
        "deliver_to": deliver_to,
        "operation": operation,
        "data": data,
    })


def in_zone(x, y):
    return ZONE_MIN_X <= x <= ZONE_MAX_X and ZONE_MIN_Y <= y <= ZONE_MAX_Y


def handle_event(id, details_str):
    details = json.loads(details_str)
    src = details.get("source")
    operation = details.get("operation")
    data = details.get("data", {})

    if src == "nav-fusion" and operation == "position":
        _last_position["x"] = data.get("x", 0.0)
        _last_position["y"] = data.get("y", 0.0)
        return

    if src == "env-assessment" and operation == "plan_path":
        target = data.get("target") or [0.0, 0.0]
        tx, ty = float(target[0]), float(target[1])
        phase = data.get("phase")

        # === ПРОВЕРКА ЦБ1 ===
        if not in_zone(tx, ty):
            print(f"[{MODULE_NAME}] BLOCKED: target ({tx},{ty}) OUTSIDE authorized zone")
            return

        if not data.get("safe_to_move", True):
            print(f"[{MODULE_NAME}] BLOCKED: {data.get('reason')}")
            return

        print(f"[{MODULE_NAME}] safe trajectory to ({tx},{ty}), phase={phase}")
        send_to("execution-controller", "execute_trajectory", {
            "task_id": data.get("task_id"),
            "target": [tx, ty],
            "phase": phase,
            "current": [_last_position["x"], _last_position["y"]],
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
