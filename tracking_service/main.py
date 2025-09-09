import os
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from pydantic import BaseModel
import asyncpg
import asyncio

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tracking-service")

app = FastAPI(title="Tracking Service")

# --- DB config
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("POSTGRES_DB", "swiftlogistics_db")
DB_USER = os.getenv("POSTGRES_USER", "swiftuser")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "swiftpassword")

# --- async DB connection pool
db_pool: asyncpg.pool.Pool | None = None

async def init_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        min_size=1,
        max_size=5
    )
    # Create table if missing
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tracking (
                id SERIAL PRIMARY KEY,
                order_id INT NOT NULL,
                status VARCHAR(100),
                updated_at TIMESTAMP DEFAULT NOW()
            );
        """)
    log.info("Tracking table initialized.")

@app.on_event("startup")
async def startup_event():
    await init_db_pool()

@app.on_event("shutdown")
async def shutdown_event():
    await db_pool.close()

# --- models
class StatusUpdate(BaseModel):
    status: str

# --- in-memory connection manager for WebSockets
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

# --- async REST endpoint to update tracking status
@app.post("/tracking/update_status/{order_id}")
async def update_status(order_id: int, update: StatusUpdate):
    try:
        async with db_pool.acquire() as conn:
            # DB insert coroutine
            db_task = conn.execute(
                "INSERT INTO tracking (order_id, status) VALUES ($1, $2)",
                order_id,
                update.status
            )

            # Broadcast coroutine
            broadcast_task = manager.broadcast(f"Order {order_id}: {update.status}")

            # Run both concurrently
            await asyncio.gather(db_task, broadcast_task)

        return {"message": "Status updated", "order_id": order_id, "status": update.status}
    except Exception as e:
        log.exception("Error updating status: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

# --- WebSocket for real-time updates
@app.websocket("/tracking/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()  # keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)
