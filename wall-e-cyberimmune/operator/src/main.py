"""
Operator — клиент оператора (FastAPI).
Эмулирует пользователя, который отправляет задание роботу через orchestrator-tasks.
Аналог mobile-client из примера хакатона.
"""
import os
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn


app = FastAPI(title="Operator Console")

ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://orchestrator-tasks:6101")


class MissionRequest(BaseModel):
    task_id: str
    token: str = "operator-token-1"
    p_x: float
    p_y: float
    d_x: float
    d_y: float


@app.post("/mission")
async def send_mission(m: MissionRequest):
    """Отправить миссию роботу."""
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(f"{ORCHESTRATOR_URL}/send_mission",
                                  json=m.model_dump(), timeout=10.0)
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"orchestrator недоступен: {e}")


@app.get("/robot_status")
async def get_robot_status():
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f"{ORCHESTRATOR_URL}/robot_status", timeout=5.0)
            return r.json()
        except Exception as e:
            return {"status": "offline", "error": str(e)}


@app.get("/ready")
async def ready():
    return {"status": "ready"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7000)
