# order_service/main.py
import os
import json
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import pika

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("order-service")

app = FastAPI(title="Order Service")

# --- environment variables for configuration
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("POSTGRES_DB", "swiftlogistics_db")
DB_USER = os.getenv("POSTGRES_USER", "swiftuser")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "swiftpassword")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
ORDER_QUEUE = os.getenv("ORDER_QUEUE", "order_queue")

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

# --- simple Order model
class Order(BaseModel):
    customer_name: str
    product: str
    quantity: int

# --- create table if missing on startup
def init_db():
    log.info("Initialising DB (creating orders table if not exists)...")
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            customer_name VARCHAR(200),
            product VARCHAR(200),
            quantity INT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    log.info("DB initialised.")

# --- publish to rabbitmq
def publish_order_message(order_id: int, order_payload: dict):
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    # durable queue so messages persist across broker restarts
    channel.queue_declare(queue=ORDER_QUEUE, durable=True)
    message = {"order_id": order_id, **order_payload}
    channel.basic_publish(
        exchange="",
        routing_key=ORDER_QUEUE,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2  # make message persistent
        )
    )
    connection.close()
    log.info("Published order_id=%s", order_id)

# --- FastAPI startup event
@app.on_event("startup")
def startup_event():
    try:
        init_db()
    except Exception as e:
        log.exception("Failed to init DB: %s", e)

# --- create order endpoint (synchronous function is ok here for learning)
@app.post("/orders/new")
def create_order(order: Order):
    try:
        # 1) save to DB
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO orders (customer_name, product, quantity) VALUES (%s, %s, %s) RETURNING id",
            (order.customer_name, order.product, order.quantity)
        )
        order_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        log.info("Saved order id=%s to DB", order_id)

        # 2) publish message
        publish_order_message(order_id, order.dict())

        return {"message": "Order created", "order_id": order_id}

    except Exception as e:
        log.exception("Error creating order: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
