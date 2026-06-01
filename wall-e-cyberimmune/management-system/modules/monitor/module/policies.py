"""
Политики безопасности — белый список разрешённых маршрутов.
Любая операция (src -> dst), не описанная в этом списке, БЛОКИРУЕТСЯ монитором.
"""

policies = (
    # === Вход оператора через orchestrator-tasks → канал связи → обработчик ===
    {"src": "orchestrator-tasks", "dst": "data-channel"},
    {"src": "data-channel",       "dst": "task-handler"},

    # === Маршрут планирования движения ===
    {"src": "task-handler",       "dst": "env-assessment"},
    {"src": "env-assessment",     "dst": "motion-planner"},
    {"src": "motion-planner",     "dst": "execution-controller"},

    # === Низкоуровневое движение ===
    {"src": "execution-controller", "dst": "tracks-control"},
    {"src": "tracks-control",       "dst": "tracks-motors"},
    {"src": "tracks-motors",        "dst": "drive-actuators"},

    # === Сенсоры навигации ===
    {"src": "nav-ins",  "dst": "nav-fusion"},
    {"src": "nav-gnss", "dst": "nav-fusion"},
    {"src": "nav-fusion", "dst": "motion-planner"},
    {"src": "nav-fusion", "dst": "execution-controller"},

    # === Лидар ===
    {"src": "lidar",            "dst": "lidar-processing"},
    {"src": "lidar-processing", "dst": "env-assessment"},

    # === Камера ===
    {"src": "camera-analyzer", "dst": "env-assessment"},

    # === Манипуляторы и крышка ===
    {"src": "task-handler",     "dst": "claws-control"},
    {"src": "claws-control",    "dst": "lid-system"},
    {"src": "claws-control",    "dst": "claws-actuators"},
    {"src": "claws-control",    "dst": "hydraulic-press"},
    {"src": "hydraulic-press",  "dst": "hydraulic-control"},

    # === Выгрузка мусора ===
    {"src": "task-handler", "dst": "trash-dump"},
    {"src": "trash-dump",   "dst": "lid-system"},

    # === Самодиагностика и состояние ===
    {"src": "fill-sensor",      "dst": "self-diagnostics"},
    {"src": "self-diagnostics", "dst": "emergency-block"},
    {"src": "battery-control",  "dst": "emergency-block"},

    # === Аварийная цепочка (доверенная) ===
    {"src": "emergency-block", "dst": "emergency-stop"},
    {"src": "emergency-block", "dst": "data-channel"},
    {"src": "emergency-stop",  "dst": "drive-actuators"},

    # === Обратная связь о завершении этапа ===
    {"src": "drive-actuators", "dst": "task-handler"},
    {"src": "claws-control",   "dst": "task-handler"},
    {"src": "trash-dump",      "dst": "task-handler"},

    # === Ответ оператору о статусе ===
    {"src": "task-handler",  "dst": "data-channel"},
    {"src": "data-channel",  "dst": "orchestrator-tasks"},

    # === Док-станция ===
    {"src": "dock-station",  "dst": "data-channel"},
)


def check_operation(id, details) -> bool:
    """Проверка возможности совершения обращения."""
    src: str = details.get("source")
    dst: str = details.get("deliver_to")

    if not all((src, dst)):
        return False

    print(f"[info] checking policies for event {id}, {src}->{dst}")
    return {"src": src, "dst": dst} in policies
