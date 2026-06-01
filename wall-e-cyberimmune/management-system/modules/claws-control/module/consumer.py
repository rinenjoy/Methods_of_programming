"""
🟡 CLAWS-CONTROL — Управление клешнями.
ЦБ1 — не активирует захват во время движения (НС-7).
ЦБ2 — координирует открытие крышки → захват → пресс → закрытие.
"""
import os
import json
import time
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


def handle_event(id, details_str):
    details = json.loads(details_str)
    src = details.get("source")
    operation = details.get("operation")
    data = details.get("data", {})

    if src == "task-handler" and operation == "grab":
        task_id = data.get("task_id")
        print(f"[{MODULE_NAME}] grab sequence for task {task_id}")

        # 1. Открыть крышку
        send_to("lid-system", "open_lid", {"task_id": task_id})
        time.sleep(0.2)
        # 2. Активировать клешни
        send_to("claws-actuators", "actuate", {"task_id": task_id, "action": "grab"})
        time.sleep(0.2)
        # 3. Запустить пресс
        send_to("hydraulic-press", "press", {"task_id": task_id})
        time.sleep(0.2)
        # 4. Закрыть крышку
        send_to("lid-system", "close_lid", {"task_id": task_id})
        time.sleep(0.2)
        # 5. Сообщить task-handler что захват окончен
        send_to("task-handler", "stage_done", {"task_id": task_id, "stage": "grabbing"})


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
