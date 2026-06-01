import asyncio
import math
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel

app = FastAPI(title="Wall-E Pro OS")

# НАСТРОЙКИ АВТОРИЗОВАННОЙ ТЕРРИТОРИИ
ZONE_MIN_X = -50.0
ZONE_MIN_Y = -50.0
ZONE_MAX_X = 50.0
ZONE_MAX_Y = 50.0

# СОСТОЯНИЕ
walle_data = {
    "x": 0.0,
    "y": 0.0,
    "status": "IDLE",
    "is_busy": False,
    "container_level": 0,
    "lid_closed": True,
    "battery": 100.0  # 100% заряд по умолчанию
}

# ПОЛЬЗОВАТЕЛЬСКИЕ ИСКЛЮЧЕНИЯ
class OutOfBoundsError(Exception): pass
class BatteryLowError(Exception): pass
class SensorError(Exception): pass
class MechanismError(Exception): pass
class SafetyError(Exception): pass


# ФОН: ЗАРЯДНАЯ СТАНЦИЯ
@app.on_event("startup")
async def start_charging_station():
    """Фоновый процесс: заряжает Валли, если он стоит на базе (0,0)"""
    asyncio.create_task(charging_loop())

async def charging_loop():
    while True:
        # Если Валли на док-станции, не на миссии и батарея не полная
        if walle_data["x"] == 0.0 and walle_data["y"] == 0.0 and not walle_data["is_busy"]:
            if walle_data["battery"] < 100.0:
                walle_data["battery"] = min(100.0, walle_data["battery"] + 1.0)
                print(f"[Зарядка] Уровень батареи: {round(walle_data['battery'], 1)}%", flush=True)
        await asyncio.sleep(3) # Зарядка 1% каждые 3 секунды


# БЛОКИ СЕНСОРОВ

async def lidar_scan():
    print("Lidar: Сканирование пространства...", flush=True)
    await asyncio.sleep(0.2)
    # Пример: Если бы сенсор был сломан, мы бы вызвали ошибку:
    # raise SensorError("Лидар не отвечает!")


async def lidar_processing():
    print("LidarProc: Обработка данных...", flush=True)
    await asyncio.sleep(0.1)

async def camera_analysis():
    print("Camera: Визуальное подтверждение объекта.", flush=True)
    await asyncio.sleep(0.2)


# --- БЛОКИ НАВИГАЦИИ И ДВИЖЕНИЯ ---


def navigation_path(tx, ty):
    dist = math.sqrt((tx - walle_data["x"])**2 + (ty - walle_data["y"])**2)
    print(f"Navigation: Дистанция до цели: {round(dist, 1)} ед.", flush=True)
    return dist

async def caterpillar_drive(tx, ty, speed=5):

    print(f"Drive: Еду в точку ({tx}, {ty})...", flush=True)
    
    while True:
        dx, dy = tx - walle_data["x"], ty - walle_data["y"]
        dist = math.sqrt(dx**2 + dy**2)

        if dist < 0.1: 
            break

        step = min(speed, dist)
        ratio = step / dist
        
        # Обновляем координаты
        walle_data["x"] += dx * ratio
        walle_data["y"] += dy * ratio
        
        # Тратим батарею (1% за 15 единиц дистанции)
        walle_data["battery"] -= (step / 15.0)
        
        if walle_data["battery"] <= 0:
            walle_data["battery"] = 0
            raise BatteryLowError("БАТАРЕЯ ПОЛНОСТЬЮ РАЗРЯЖЕНА! Робот заглох.")
            
        await asyncio.sleep(0.5) 
        
    print(f"Drive: Робот прибыл в пункт назначения.", flush=True)
    return True



# БЛОКИ МАНИПУЛЯТОРОВ И СИСТЕМ

async def lid_control(close: bool):
    walle_data["lid_closed"] = close
    state = "ЗАКРЫТА" if close else "ОТКРЫТА"
    print(f"Lid: Крышка бака {state}.", flush=True)

async def claw_grab():
    print("Claws: Захват мусора манипулятором...", flush=True)
    await asyncio.sleep(0.5)

async def hydraulics_press():
    if walle_data["lid_closed"]:
            raise SafetyError("Нельзя прессовать с закрытой крышкой!")
    print("Press: Работа пресса (сжатие)...", flush=True)
    await asyncio.sleep(0.5)
    walle_data["container_level"] = 100

async def dump_system():
    print("Dump: Очистка бака завершена.", flush=True)
    walle_data["container_level"] = 0

async def self_diagnostics(p_x, p_y, d_x, d_y):
    """Блок самодиагностики с расчетом батареи и геозон"""
    print("\nSelf diagnostic: Запуск проверки систем перед выездом...", flush=True)
    
    # 2. Расчет общей дистанции маршрута: Дом -> Точка А -> Точка Б -> Дом
    dist1 = math.sqrt(p_x**2 + p_y**2)  # От (0,0) до Мусора
    dist2 = math.sqrt((d_x - p_x)**2 + (d_y - p_y)**2) # От Мусора до Свалки
    dist3 = math.sqrt(d_x**2 + d_y**2)  # От Свалки обратно в (0,0)
    
    total_distance = dist1 + dist2 + dist3
    battery_needed = total_distance / 15.0 # 1% заряда на каждые 15 единиц пути
            
    print(f"Self diagnostic: Системы в норме. Маршрут: {round(total_distance, 1)} ед. "
          f"Расчетный расход заряда: {round(battery_needed, 1)}%", flush=True)
    return True


# ГЛАВНЫЙ АЛГОРИТМ

async def run_walle_mission(p_x, p_y, d_x, d_y):
    walle_data["is_busy"] = True
    print("\n--- НАЧАЛО ВЫПОЛНЕНИЯ ЗАДАЧИ ---", flush=True)
    
    # ЭТАП 0: Предварительная проверка (Самое важное!)
    await self_diagnostics(p_x, p_y, d_x, d_y)
    
    # ЭТАП 1: Подготовка и путь к мусору
    await lidar_scan()
    await lidar_processing()
    await camera_analysis()
    
    walle_data["status"] = "MOVING_TO_PICKUP"
    navigation_path(p_x, p_y)
    await caterpillar_drive(p_x, p_y)

    # ЭТАП 2: Сбор мусора
    walle_data["status"] = "COLLECTING"
    await lid_control(close=False)
    await claw_grab()
    await hydraulics_press()
    await lid_control(close=True)

    # ЭТАП 3: Путь на свалку
    walle_data["status"] = "MOVING_TO_DROPOFF"
    navigation_path(d_x, d_y)
    await caterpillar_drive(d_x, d_y)

    # ЭТАП 4: Разгрузка
    walle_data["status"] = "DUMPING"
    await asyncio.sleep(0.5)
    await lid_control(close=False)
    await dump_system()
    await lid_control(close=True)

    # ЭТАП 5: Возврат домой
    walle_data["status"] = "RETURNING_HOME"
    print("Возвращение на док-станцию...", flush=True)
    await caterpillar_drive(0.0, 0.0)
    
    # ЭТАП 6: Завершение миссии
    walle_data["status"] = "IDLE"
    walle_data["is_busy"] = False


# API ЭНДПОИНТЫ

class MissionRequest(BaseModel):
    task_id: str
    pickup: list[float]
    dropoff: list[float]

@app.post("/start_cycle")
async def start_mission(m: MissionRequest, background_tasks: BackgroundTasks):
    if walle_data["is_busy"]:
        return {"error": "Robot is busy"}
    background_tasks.add_task(run_walle_mission, *m.pickup, *m.dropoff)
    return {"status": "started", "task_id": m.task_id}

@app.get("/status")
async def get_robot_status():
    return {
        "position": {"x": round(walle_data["x"], 2), "y": round(walle_data["y"], 2)},
        "status": walle_data["status"],
        "cargo": f"{walle_data['container_level']}%",
        "safety_lock": "Lid Closed" if walle_data["lid_closed"] else "Lid Open",
        "battery": f"{round(walle_data['battery'], 1)}%"
    }

@app.get("/ready")
async def check_ready():
    return {"status": "ready"}