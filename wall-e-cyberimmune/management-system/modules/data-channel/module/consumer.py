"""
🔴 DATA-CHANNEL — Среда передачи данных (НЕДОВЕРЕННАЯ).
Простой транспорт: получает из orchestrator-tasks → передаёт task-handler.
Получает статусы от task-handler/emergency-block → передаёт orchestrator-tasks.
"""
import os
import json
import threading

from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")


def handle_event(id, details_str):
    details = json.loads(details_str)
    src = details.get("source")
    operation = details.get("operation")

    print(f"[{MODULE_NAME}] forwarding from {src}: {operation}")

    if src == "orchestrator-tasks":
        # вход от оператора → передаём обработчику заданий
        details["deliver_to"] = "task-handler"
        details["operation"] = "new_mission"
        proceed_to_deliver(id, details)
    elif src in ("task-handler", "emergency-block", "dock-station"):
        # исходящие наружу: статус/алерт → оператору
        details["deliver_to"] = "orchestrator-tasks"
        proceed_to_deliver(id, details)


def consumer_job(args, config):
    consumer = Consumer(config)

    def reset_offset(verifier_consumer, partitions):
        if not args.reset:
            return
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        verifier_consumer.assign(partitions)

    consumer.subscribe([MODULE_NAME], on_assign=reset_offset)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                id = msg.key().decode("utf-8")
                details_str = msg.value().decode("utf-8")
                handle_event(id, details_str)
            except Exception as e:
                print(f"[error] {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def start_consumer(args, config):
    print(f"{MODULE_NAME}_consumer started")
    threading.Thread(target=lambda: consumer_job(args, config), daemon=True).start()
