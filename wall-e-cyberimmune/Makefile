.PHONY: all build up logs clean test mission status

all: build up

build:
	docker compose build

up:
	docker compose up -d

logs:
	docker compose logs -f --tail=100

logs-monitor:
	docker compose logs -f monitor

logs-orchestrator:
	docker compose logs -f orchestrator-tasks

clean:
	docker compose down -v
	docker compose rm -fsv

restart:
	docker compose restart

# === Готовые сценарии ===

# Отправка типового задания
mission:
	curl -X POST http://localhost:7000/mission \
		-H "Content-Type: application/json" \
		-d '{"task_id":"M-001","token":"operator-token-1","p_x":10,"p_y":10,"d_x":45,"d_y":45}' \
		| python3 -m json.tool

# Проверка статуса
status:
	@echo "=== Robot status (via operator) ==="
	@curl -s http://localhost:7000/robot_status | python3 -m json.tool
	@echo "\n=== Physical state (wall-e direct) ==="
	@curl -s http://localhost:8000/status | python3 -m json.tool

# Проверка готовности всех сервисов
ready:
	@curl -s http://localhost:7000/ready && echo " ← operator"
	@curl -s http://localhost:6101/ready && echo " ← orchestrator-tasks"
	@curl -s http://localhost:8000/ready && echo " ← wall-e"

# Симуляция атаки — отправка из неавторизованного модуля
# (мы шлём прямо в kafka-топик task-handler из не-data-channel; монитор должен блокировать)
test-security:
	@echo "Этот скрипт демонстрирует блокировку запрещённого маршрута."
	@echo "Смотрите логи: make logs-monitor"
