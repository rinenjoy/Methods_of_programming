"""
🟡 NAV-FUSION — Комплексирование навигации.
ЦБ1 — слияние данных ИНС и GNSS с проверкой расхождения (защита от спуфинга).
"""
import os
import json
import threading
import math

from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")

_ins = {"x": 0.0, "y": 0.0}
_gnss = {"x": 0.0, "y": 0.0}
MAX_DEVIATION = 5.0  # м, расхождение между ИНС и GNSS


def send_to(deliver_to, operation, data):
    proceed_to_deliver(uuid4().__str__(), {
        "deliver_to": deliver_to,
        "operation": operation,
        "data": data,
    })


def publish_position():
    dx = _ins["x"] - _gnss["x"]
    dy = _ins["y"] - _gnss["y"]
    dev = math.sqrt(dx * dx + dy * dy)

    # === Защита от спуфинга GNSS (НС-3) ===
    if dev > MAX_DEVIATION:
        # доверяем ИНС
        pos = {"x": _ins["x"], "y": _ins["y"], "source": "ins_only", "deviation": dev}
        print(f"[{MODULE_NAME}] GNSS deviation={dev:.2f} > {MAX_DEVIATION}, trusting INS")
    else:
        # среднее
        pos = {
            "x": (_ins["x"] + _gnss["x"]) / 2,
            "y": (_ins["y"] + _gnss["y"]) / 2,
            "source": "fused",
            "deviation": dev,
        }
    send_to("motion-planner", "position", pos)
    send_to("execution-controller", "position", pos)


def handle_event(id, details_str):
    details = json.loads(details_str)
    operation = details.get("operation")
    data = details.get("data", {})

    if operation == "ins_data":
        _ins["x"] = data.get("x", 0.0)
        _ins["y"] = data.get("y", 0.0)
        publish_position()
    elif operation == "gnss_data":
        _gnss["x"] = data.get("x", 0.0)
        _gnss["y"] = data.get("y", 0.0)


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
