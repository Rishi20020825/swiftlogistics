# transaction_service/main.py
import os
import asyncio
import json
import aio_pika
import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional, Any, Set
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta
from enum import Enum
import uuid
from contextlib import asynccontextmanager
from tenacity import retry, stop_after_attempt, wait_exponential

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define transaction states
class TransactionState(str, Enum):
    PENDING = "PENDING"
    STARTED = "STARTED"
    COMMITTED = "COMMITTED"
    COMPENSATING = "COMPENSATING"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"

# Define service states
class ServiceState(str, Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATED = "COMPENSATED"

# Define models
class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String, unique=True, index=True)
    order_id = Column(String, index=True)
    state = Column(String, default=TransactionState.PENDING)
    services = Column(JSON)  # List of services participating in this transaction
    start_time = Column(DateTime, default=datetime.utcnow)
    update_time = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completion_time = Column(DateTime, nullable=True)
    error_message = Column(String, nullable=True)
    retry_count = Column(Integer, default=0)

# Initialize database
Base.metadata.create_all(bind=engine)

# Pydantic models
class Service(BaseModel):
    name: str
    state: ServiceState = ServiceState.PENDING
    compensation_required: bool = True
    compensation_successful: Optional[bool] = None
    details: Optional[Dict[str, Any]] = None

class TransactionCreate(BaseModel):
    order_id: str
    services: List[Service]

class TransactionResponse(BaseModel):
    transaction_id: str
    order_id: str
    state: TransactionState
    services: List[Service]
    start_time: datetime
    update_time: datetime
    completion_time: Optional[datetime] = None
    error_message: Optional[str] = None

class ServiceUpdate(BaseModel):
    service_name: str
    state: ServiceState
    details: Optional[Dict[str, Any]] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup: Create DB tables and start background tasks
    Base.metadata.create_all(bind=engine)
    
    # Start the transaction recovery task
    app.state.recovery_task = asyncio.create_task(periodic_transaction_recovery())
    
    yield
    
    # Cleanup: Cancel background tasks
    app.state.recovery_task.cancel()
    try:
        await app.state.recovery_task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# The set of services in our system
SYSTEM_SERVICES = {
    "order_service": {
        "compensation_url": "/orders/{order_id}/cancel",
        "requires_compensation": True
    },
    "cms_adapter": {
        "compensation_url": "/cms/orders/{order_id}/cancel",
        "requires_compensation": True
    },
    "ros_adapter": {
        "compensation_url": "/ros/routes/by-order/{order_id}/cancel",
        "requires_compensation": True
    },
    "wms_adapter": {
        "compensation_url": "/wms/shipments/by-order/{order_id}/cancel",
        "requires_compensation": True
    }
}

# Function to get DB session
def get_db():
    db = SessionLocal()
    try:
        return db
    finally:
        db.close()

async def update_transaction_state(transaction_id: str, new_state: TransactionState, error_message: Optional[str] = None):
    """Update a transaction's state"""
    db = get_db()
    transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    
    if not transaction:
        db.close()
        return False
    
    transaction.state = new_state
    transaction.update_time = datetime.utcnow()
    
    if new_state in [TransactionState.COMPLETED, TransactionState.FAILED]:
        transaction.completion_time = datetime.utcnow()
    
    if error_message:
        transaction.error_message = error_message
        
    db.commit()
    db.close()
    return True

async def update_service_state(transaction_id: str, service_name: str, new_state: ServiceState, details: Optional[Dict[str, Any]] = None):
    """Update a service's state within a transaction"""
    db = get_db()
    transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    
    if not transaction:
        db.close()
        return False
    
    services = transaction.services
    for service in services:
        if service["name"] == service_name:
            service["state"] = new_state
            if details:
                service["details"] = details
            break
    
    transaction.services = services
    transaction.update_time = datetime.utcnow()
    db.commit()
    db.close()
    return True

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
async def call_service_compensation(service_name: str, order_id: str):
    """Call the compensation endpoint for a service"""
    if service_name not in SYSTEM_SERVICES:
        return False
    
    service_info = SYSTEM_SERVICES[service_name]
    if not service_info.get("requires_compensation", False):
        return True
    
    compensation_url = service_info["compensation_url"].format(order_id=order_id)
    base_url = f"http://{service_name}:8000"  # Assuming all services run on port 8000
    full_url = f"{base_url}{compensation_url}"
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                full_url,
                timeout=10.0
            )
            return response.status_code < 400
    except Exception as e:
        print(f"Error calling compensation for {service_name}: {e}")
        return False

async def compensate_transaction(transaction_id: str):
    """Attempt to compensate (rollback) a transaction"""
    db = get_db()
    transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    
    if not transaction or transaction.state not in [TransactionState.STARTED, TransactionState.FAILED]:
        db.close()
        return False
    
    # Update transaction state
    transaction.state = TransactionState.COMPENSATING
    db.commit()
    
    order_id = transaction.order_id
    services = transaction.services
    
    # Sort services in reverse order for compensation
    services_to_compensate = sorted(
        [s for s in services if s["state"] == ServiceState.COMPLETED and s.get("compensation_required", True)],
        key=lambda s: services.index(s),
        reverse=True
    )
    
    all_successful = True
    
    for service in services_to_compensate:
        service_name = service["name"]
        success = await call_service_compensation(service_name, order_id)
        
        service["compensation_successful"] = success
        if not success:
            all_successful = False
    
    # Update the transaction with compensation results
    transaction.services = services
    transaction.state = TransactionState.FAILED
    transaction.completion_time = datetime.utcnow()
    transaction.update_time = datetime.utcnow()
    
    if not all_successful:
        transaction.error_message = "Some compensation actions failed. Manual intervention may be required."
    
    db.commit()
    db.close()
    
    # Publish a message about the compensation
    await publish_event("transaction_compensated", {
        "transaction_id": transaction_id,
        "order_id": order_id,
        "success": all_successful,
        "timestamp": datetime.utcnow().isoformat()
    })
    
    return all_successful

async def check_transaction_completion(transaction_id: str):
    """Check if all services in a transaction have completed"""
    db = get_db()
    transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    
    if not transaction or transaction.state != TransactionState.STARTED:
        db.close()
        return
    
    services = transaction.services
    all_completed = all(service["state"] == ServiceState.COMPLETED for service in services)
    any_failed = any(service["state"] == ServiceState.FAILED for service in services)
    
    if all_completed:
        transaction.state = TransactionState.COMPLETED
        transaction.completion_time = datetime.utcnow()
        transaction.update_time = datetime.utcnow()
        db.commit()
        
        # Publish a message about the completed transaction
        await publish_event("transaction_completed", {
            "transaction_id": transaction_id,
            "order_id": transaction.order_id,
            "timestamp": datetime.utcnow().isoformat()
        })
        
    elif any_failed:
        transaction.state = TransactionState.FAILED
        transaction.update_time = datetime.utcnow()
        db.commit()
        
        # Start compensation in the background
        asyncio.create_task(compensate_transaction(transaction_id))
    
    db.close()

async def publish_event(event_type: str, data: Dict[str, Any]):
    """Publish an event to RabbitMQ"""
    try:
        rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
        connection = await aio_pika.connect_robust(rabbitmq_url)
        
        async with connection:
            channel = await connection.channel()
            
            # Declare the exchange
            exchange = await channel.declare_exchange(
                "transaction_events", 
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            
            # Create the message
            message_body = json.dumps({
                "event_type": event_type,
                "data": data,
                "timestamp": datetime.utcnow().isoformat()
            }).encode()
            
            message = aio_pika.Message(
                body=message_body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )
            
            # Publish the message
            await exchange.publish(
                message,
                routing_key=event_type
            )
            
            print(f"Published event: {event_type}")
            
    except Exception as e:
        print(f"Error publishing event: {e}")

async def periodic_transaction_recovery():
    """Periodically check for stalled transactions and attempt recovery"""
    while True:
        try:
            await recover_stalled_transactions()
        except Exception as e:
            print(f"Error in transaction recovery: {e}")
        
        # Run every 5 minutes
        await asyncio.sleep(300)

async def recover_stalled_transactions():
    """Find and recover stalled transactions"""
    db = get_db()
    
    # Find transactions that have been in STARTED state for too long (30 minutes)
    stalled_time = datetime.utcnow() - timedelta(minutes=30)
    stalled_transactions = db.query(Transaction).filter(
        Transaction.state == TransactionState.STARTED,
        Transaction.update_time < stalled_time
    ).all()
    
    # Find transactions that have been in COMPENSATING state for too long (30 minutes)
    compensating_transactions = db.query(Transaction).filter(
        Transaction.state == TransactionState.COMPENSATING,
        Transaction.update_time < stalled_time
    ).all()
    
    db.close()
    
    # Process stalled transactions
    for transaction in stalled_transactions:
        print(f"Recovering stalled transaction: {transaction.transaction_id}")
        transaction.retry_count += 1
        
        # If retried too many times, mark as failed and attempt compensation
        if transaction.retry_count > 3:
            await update_transaction_state(
                transaction.transaction_id, 
                TransactionState.FAILED,
                "Transaction stalled and exceeded retry limit"
            )
            await compensate_transaction(transaction.transaction_id)
        else:
            # Check if it's actually complete
            await check_transaction_completion(transaction.transaction_id)
    
    # Log compensating transactions that are stuck
    for transaction in compensating_transactions:
        print(f"Warning: Transaction {transaction.transaction_id} has been in COMPENSATING state for too long")
        
        # Mark with an error message but keep the state
        db = get_db()
        trans = db.query(Transaction).filter(Transaction.transaction_id == transaction.transaction_id).first()
        if trans:
            trans.error_message = "Compensation process stalled. Manual intervention required."
            trans.update_time = datetime.utcnow()
            db.commit()
        db.close()

@app.post("/transactions", response_model=TransactionResponse)
async def create_transaction(transaction: TransactionCreate, background_tasks: BackgroundTasks):
    """Create a new distributed transaction"""
    transaction_id = f"tx-{uuid.uuid4().hex[:8]}"
    
    # Convert the Pydantic services to dictionaries
    services_dict = [service.dict() for service in transaction.services]
    
    # Create the transaction
    db = get_db()
    db_transaction = Transaction(
        transaction_id=transaction_id,
        order_id=transaction.order_id,
        state=TransactionState.PENDING,
        services=services_dict
    )
    
    db.add(db_transaction)
    db.commit()
    db.refresh(db_transaction)
    db.close()
    
    # Start the transaction (set to STARTED state)
    await update_transaction_state(transaction_id, TransactionState.STARTED)
    
    # Publish event for transaction created
    background_tasks.add_task(
        publish_event,
        "transaction_created",
        {
            "transaction_id": transaction_id,
            "order_id": transaction.order_id,
            "services": services_dict
        }
    )
    
    return TransactionResponse(
        transaction_id=transaction_id,
        order_id=transaction.order_id,
        state=TransactionState.STARTED,
        services=transaction.services,
        start_time=db_transaction.start_time,
        update_time=db_transaction.update_time
    )

@app.put("/transactions/{transaction_id}/services", response_model=TransactionResponse)
async def update_service(transaction_id: str, update: ServiceUpdate, background_tasks: BackgroundTasks):
    """Update a service state within a transaction"""
    db = get_db()
    transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    
    if not transaction:
        db.close()
        raise HTTPException(status_code=404, detail="Transaction not found")
    
    # Find and update the service
    services = transaction.services
    service_found = False
    
    for service in services:
        if service["name"] == update.service_name:
            service["state"] = update.state
            if update.details:
                service["details"] = update.details
            service_found = True
            break
    
    if not service_found:
        db.close()
        raise HTTPException(status_code=404, detail=f"Service {update.service_name} not found in transaction")
    
    # Update the transaction
    transaction.services = services
    transaction.update_time = datetime.utcnow()
    db.commit()
    
    # Check if the transaction is now complete or needs compensation
    background_tasks.add_task(check_transaction_completion, transaction_id)
    
    # Convert the DB model to Pydantic model for response
    services_pydantic = [Service(**service) for service in transaction.services]
    
    response = TransactionResponse(
        transaction_id=transaction.transaction_id,
        order_id=transaction.order_id,
        state=transaction.state,
        services=services_pydantic,
        start_time=transaction.start_time,
        update_time=transaction.update_time,
        completion_time=transaction.completion_time,
        error_message=transaction.error_message
    )
    
    db.close()
    return response

@app.get("/transactions/{transaction_id}", response_model=TransactionResponse)
async def get_transaction(transaction_id: str):
    """Get a transaction by ID"""
    db = get_db()
    transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    
    if not transaction:
        db.close()
        raise HTTPException(status_code=404, detail="Transaction not found")
    
    # Convert the DB model to Pydantic model for response
    services_pydantic = [Service(**service) for service in transaction.services]
    
    response = TransactionResponse(
        transaction_id=transaction.transaction_id,
        order_id=transaction.order_id,
        state=transaction.state,
        services=services_pydantic,
        start_time=transaction.start_time,
        update_time=transaction.update_time,
        completion_time=transaction.completion_time,
        error_message=transaction.error_message
    )
    
    db.close()
    return response

@app.get("/transactions/by-order/{order_id}", response_model=List[TransactionResponse])
async def get_transactions_by_order(order_id: str):
    """Get all transactions for an order"""
    db = get_db()
    transactions = db.query(Transaction).filter(Transaction.order_id == order_id).all()
    
    if not transactions:
        db.close()
        return []
    
    response = []
    for transaction in transactions:
        services_pydantic = [Service(**service) for service in transaction.services]
        
        response.append(TransactionResponse(
            transaction_id=transaction.transaction_id,
            order_id=transaction.order_id,
            state=transaction.state,
            services=services_pydantic,
            start_time=transaction.start_time,
            update_time=transaction.update_time,
            completion_time=transaction.completion_time,
            error_message=transaction.error_message
        ))
    
    db.close()
    return response

@app.post("/transactions/{transaction_id}/compensate")
async def compensate_transaction_endpoint(transaction_id: str, background_tasks: BackgroundTasks):
    """Manually trigger compensation for a transaction"""
    db = get_db()
    transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    
    if not transaction:
        db.close()
        raise HTTPException(status_code=404, detail="Transaction not found")
    
    if transaction.state == TransactionState.COMPLETED:
        db.close()
        raise HTTPException(status_code=400, detail="Cannot compensate a completed transaction")
    
    if transaction.state == TransactionState.COMPENSATING:
        db.close()
        raise HTTPException(status_code=400, detail="Transaction is already being compensated")
    
    db.close()
    
    # Start compensation in the background
    background_tasks.add_task(compensate_transaction, transaction_id)
    
    return {"message": f"Compensation started for transaction {transaction_id}"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}
