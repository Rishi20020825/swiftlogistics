# swiftlogistics/cms_adapter/main.py
import os
import asyncio
import json
import aio_pika
from fastapi import FastAPI
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Database setup (same as Core Services team)
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
                print(f"CMS Adapter: Processing order {order_data['order_id']}")

                # --- SIMULATED LEGACY SYSTEM CALL (SOAP/XML) ---
                # Simulate a network delay for the API call
                await asyncio.sleep(2) 
                print(f"CMS Adapter: Order {order_data['order_id']} created in CMS.")

                # Update the database to reflect the new status
                db = SessionLocal()
                order = db.query(Order).get(order_data['order_id'])
                if order:
                    order.status = "CMS-confirmed"
                    db.commit()
                db.close()

@app.on_event("startup")
async def startup_event():
    # Start the consumer as a background task to not block the main app
    asyncio.create_task(consume_messages())