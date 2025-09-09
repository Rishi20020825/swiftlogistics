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


DATABASE_URL = os.getenv("DATABASE_URL")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
ORDER_QUEUE = os.getenv("ORDER_QUEUE", "order_queue")

# --- simple Order model
class Order(BaseModel):
    customer_name: str
    product: str
    quantity: int
    address: str

def get_db_conn():
    try:
        log.info(f"Connecting to database with URL: {DATABASE_URL}")
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        log.error(f"Database connection error: {e}")
        raise

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
            address VARCHAR(500),
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    log.info("DB initialised.")

# --- publish to rabbitmq
def publish_order_message(order_id: int, order_payload: dict):
    try:
        log.info(f"Connecting to RabbitMQ at {RABBITMQ_HOST}")
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST, 
            credentials=credentials,
            connection_attempts=3,
            retry_delay=5
        )
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
        log.info(f"Published order_id={order_id} to RabbitMQ")
    except Exception as e:
        log.error(f"Failed to publish message to RabbitMQ: {e}")
        # Don't re-raise so the API can still return success for the DB operation
        # In production, you might want to implement a retry mechanism or queue locally

# --- FastAPI startup event
@app.on_event("startup")
def startup_event():
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            log.info(f"Attempting to initialize database (attempt {retry_count + 1}/{max_retries})")
            init_db()
            log.info("Database initialization successful")
            return
        except Exception as e:
            retry_count += 1
            log.error(f"Failed to init DB (attempt {retry_count}/{max_retries}): {e}")
            
            if retry_count < max_retries:
                wait_time = 5 * retry_count  # Increasing backoff
                log.info(f"Retrying in {wait_time} seconds...")
                import time
                time.sleep(wait_time)
            else:
                log.error(f"Maximum retries reached. Could not initialize database after {max_retries} attempts.")

# --- create order endpoint (synchronous function is ok here for learning)
@app.post("/orders/new")
def create_order(order: Order):
    try:
        # 1) save to DB
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO orders (customer_name, product, quantity, address) VALUES (%s, %s, %s, %s) RETURNING id",
            (order.customer_name, order.product, order.quantity, order.address)
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
