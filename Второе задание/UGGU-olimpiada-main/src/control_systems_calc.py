import math

CYBER_OBSTACLE = False

def update_speed_and_direction(current_pos, target_pos, current_speed, current_direction, obstacle_distances, max_speed):
    current_x, current_y = current_pos
    target_x, target_y = target_pos
    stop_radius = 10
    safe_distance = 24
    num_sectors = 6
    sector_angle = 360 / num_sectors

    dx = target_x - current_x
    dy = target_y - current_y
    distance_to_target = math.hypot(dx, dy)

    if distance_to_target <= stop_radius:
        return 0, current_direction, "success"

    # Расчет направления к цели: учёт координатной системы (x вправо, y вниз)
    angle_to_target = math.degrees(math.atan2(dx, -dy)) % 360
    target_sector = int(angle_to_target // sector_angle) % num_sectors

    # Корректное ускорение/торможение
    if distance_to_target < 1:
        new_speed = 0
    else:
        new_speed = min(current_speed + 2.0, max_speed)

    braking_distance = max(12, new_speed * 2)
    if distance_to_target < braking_distance:
        new_speed = max(0, min(new_speed, distance_to_target / 2))

    # Объезд препятствий
    if len(obstacle_distances) == num_sectors and obstacle_distances[target_sector] < safe_distance:
        new_speed = max(1, new_speed * 0.6)
        # корректируем направление немного в сторону, если рядом препятствие
        angle_to_target = (angle_to_target + 20) % 360

    if CYBER_OBSTACLE:
        angle_to_target = (angle_to_target - 20) % 360

    new_direction = round(angle_to_target, 1)
    return new_speed, new_direction, "moved"
