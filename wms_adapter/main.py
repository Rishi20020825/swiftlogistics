# swiftlogistics/wms_adapter/main.py
import os
import asyncio
import json
import aio_pika
from fastapi import FastAPI
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Database setup (copy the same)
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(String)
    item_description = Column(String)
    status = Column(String, default="pending")

app = FastAPI()

async def consume_messages():
    connection = await aio_pika.connect_robust(os.getenv("RABBITMQ_URL"))
    channel = await connection.channel()
    queue = await channel.declare_queue("order_queue", durable=True)
    
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                order_data = json.loads(message.body.decode())
                print(f"WMS Adapter: Processing order {order_data['order_id']}")
                
                # --- SIMULATED LEGACY SYSTEM CALL (Proprietary TCP/IP) ---
                # A longer mock delay to reflect a more complex process
                await asyncio.sleep(3) 
                print(f"WMS Adapter: Package for order {order_data['order_id']} is ready.")
                
                db = SessionLocal()
                order = db.query(Order).get(order_data['order_id'])
                if order:
                    order.status = "WMS-ready"
                    db.commit()
                db.close()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume_messages())