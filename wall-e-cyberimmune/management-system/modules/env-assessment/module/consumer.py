"""
🟡 ENV-ASSESSMENT — Модуль оценки окружающей среды.
ЦБ1 — гарантирует, что робот не наедет на людей/животных/инфраструктуру.
Получает данные от lidar-processing и camera-analyzer,
агрегирует и передаёт планировщику.
"""
import os
import json
import threading

from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")

# Кэш последних данных от сенсоров
_obstacles = []   # от lidar-processing
_objects = []     # от camera-analyzer
_pending_request = None


def send_to(deliver_to, operation, data):
    proceed_to_deliver(uuid4().__str__(), {
        "deliver_to": deliver_to,
        "operation": operation,
        "data": data,
    })


def is_safe(obstacles, objects):
    """Проверка ЦБ1: нет ли людей/животных на пути."""
    for obj in objects:
        if obj.get("class") in ("human", "animal"):
            return False, f"обнаружен {obj.get('class')}"
    return True, "OK"


def handle_event(id, details_str):
    global _pending_request, _obstacles, _objects
    details = json.loads(details_str)
    src = details.get("source")
    operation = details.get("operation")
    data = details.get("data", {})

    print(f"[{MODULE_NAME}] from {src}: {operation}")

    if src == "task-handler" and operation == "scan_environment":
        # сохраняем запрос и используем последние данные сенсоров
        _pending_request = data
        # обращаемся к планировщику с агрегированной картой
        safe, reason = is_safe(_obstacles, _objects)
        send_to("motion-planner", "plan_path", {
            "task_id": data.get("task_id"),
            "target": data.get("target"),
            "phase": data.get("phase"),
            "obstacles": _obstacles,
            "objects": _objects,
            "safe_to_move": safe,
            "reason": reason,
        })

    elif src == "lidar-processing" and operation == "obstacle_map":
        _obstacles = data.get("obstacles", [])

    elif src == "camera-analyzer" and operation == "objects":
        _objects = data.get("objects", [])


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
