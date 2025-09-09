# tracking_service/main.py
import os
import asyncio
import json
import aio_pika
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional, Any
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import uuid
from contextlib import asynccontextmanager

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define models
class TrackingEvent(Base):
    __tablename__ = "tracking_events"
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True)
    order_id = Column(String, index=True)
    shipment_id = Column(String, index=True, nullable=True)
    route_id = Column(String, index=True, nullable=True)
    event_type = Column(String)  # ORDER_CREATED, SHIPMENT_CREATED, ROUTE_CREATED, LOCATION_UPDATE, etc.
    status = Column(String)      # Order Placed, Order Processed, Picked Up, In Transit, Out for Delivery, Delivered, Delayed, Returned
    location = Column(String, nullable=True)
    description = Column(String, nullable=True)
    updated_by = Column(String, nullable=True)  # Driver ID or system ID
    event_data = Column(JSON)
    timestamp = Column(DateTime, default=datetime.utcnow)

# Initialize database
Base.metadata.create_all(bind=engine)

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        # Main connection dictionary: {order_id: [connection1, connection2, ...]}
        self.active_connections: Dict[str, List[WebSocket]] = {}
        
    async def connect(self, websocket: WebSocket, order_id: str):
        await websocket.accept()
        if order_id not in self.active_connections:
            self.active_connections[order_id] = []
        self.active_connections[order_id].append(websocket)
        
    def disconnect(self, websocket: WebSocket, order_id: str):
        if order_id in self.active_connections:
            if websocket in self.active_connections[order_id]:
                self.active_connections[order_id].remove(websocket)
            if not self.active_connections[order_id]:
                # Clean up empty lists
                del self.active_connections[order_id]
    
    async def send_message(self, message: Dict[str, Any], order_id: str):
        if order_id in self.active_connections:
            for connection in self.active_connections[order_id]:
                await connection.send_json(message)
    
    async def broadcast_message(self, message: Dict[str, Any]):
        """Send a message to all connected clients"""
        for order_connections in self.active_connections.values():
            for connection in order_connections:
                await connection.send_json(message)


manager = ConnectionManager()

# Pydantic models
class TrackingEventCreate(BaseModel):
    order_id: str
    shipment_id: Optional[str] = None
    route_id: Optional[str] = None
    event_type: str
    status: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None
    updated_by: Optional[str] = None
    event_data: Dict[str, Any]

class TrackingEventResponse(BaseModel):
    event_id: str
    order_id: str
    shipment_id: Optional[str] = None
    route_id: Optional[str] = None
    event_type: str
    status: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None
    updated_by: Optional[str] = None
    event_data: Dict[str, Any]
    timestamp: datetime

class StatusUpdateRequest(BaseModel):
    order_id: str
    status: str
    location: Optional[str] = None
    description: Optional[str] = None
    driver_id: Optional[str] = None
    notes: Optional[str] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup: Create DB tables
    Base.metadata.create_all(bind=engine)
    
    # Start the RabbitMQ consumer
    asyncio.create_task(consume_messages())
    
    yield
    
    # Cleanup: Close connections, etc.
    # Could close DB connections or message brokers here

app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def consume_messages():
    """
    Consume messages from RabbitMQ and update tracking information.
    This will listen to multiple queues for different event types.
    """
    try:
        rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
        print(f"Tracking Service: Connecting to RabbitMQ at: {rabbitmq_url}")
        connection = await aio_pika.connect_robust(rabbitmq_url)
        channel = await connection.channel()
        
        # Declare queues for different event types
        order_queue = await channel.declare_queue("order_events", durable=True)
        shipment_queue = await channel.declare_queue("shipment_events", durable=True)
        route_queue = await channel.declare_queue("route_events", durable=True)
        status_queue = await channel.declare_queue("status_update_events", durable=True)
        
        print("Tracking Service: Successfully connected to RabbitMQ")
        
        # Store connection and channel globally
        app.state.rabbitmq_connection = connection
        app.state.rabbitmq_channel = channel
        
        # Start consumers for each queue
        asyncio.create_task(process_order_events(order_queue))
        asyncio.create_task(process_shipment_events(shipment_queue))
        asyncio.create_task(process_route_events(route_queue))
        asyncio.create_task(process_status_updates(status_queue))
        
        # Also consume from the main order queue to track initial orders
        main_order_queue = await channel.declare_queue("order_queue", durable=True)
        asyncio.create_task(process_main_order_queue(main_order_queue))
        
    except Exception as e:
        print(f"Tracking Service: Error connecting to RabbitMQ: {e}")
        print("Will retry in 10 seconds...")
        await asyncio.sleep(10)
        asyncio.create_task(consume_messages())  # Retry connecting

async def process_main_order_queue(queue):
    """Process the main order queue to capture initial order creation events"""
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                try:
                    order_data = json.loads(message.body.decode())
                    print(f"Tracking Service: Processing new order {order_data.get('order_id')}")
                    
                    # Create tracking event for order creation
                    event = TrackingEvent(
                        event_id=f"evt-{uuid.uuid4().hex[:8]}",
                        order_id=order_data.get('order_id'),
                        event_type="ORDER_CREATED",
                        status="Order Placed",
                        location=order_data.get('delivery_address', 'Online'),
                        description="Your order has been received and is being processed",
                        updated_by="system",
                        event_data=order_data
                    )
                    
                    db = SessionLocal()
                    db.add(event)
                    db.commit()
                    
                    # Notify any websocket clients listening for this order
                    await manager.send_message({
                        "event_type": "ORDER_CREATED",
                        "order_id": order_data.get('order_id'),
                        "status": "Order Placed",
                        "timestamp": datetime.utcnow().isoformat(),
                        "data": order_data
                    }, order_data.get('order_id'))
                    
                    db.close()
                except Exception as e:
                    print(f"Error processing order from main queue: {e}")

async def process_order_events(queue):
    """Process messages from the order events queue"""
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                try:
                    event_data = json.loads(message.body.decode())
                    print(f"Tracking Service: Processing order event for {event_data.get('order_id')}")
                    
                    # Create tracking event
                    event = TrackingEvent(
                        event_id=f"evt-{uuid.uuid4().hex[:8]}",
                        order_id=event_data.get('order_id'),
                        event_type=event_data.get('event_type', 'ORDER_UPDATE'),
                        event_data=event_data
                    )
                    
                    db = SessionLocal()
                    db.add(event)
                    db.commit()
                    
                    # Notify websocket clients
                    await manager.send_message({
                        "event_type": event_data.get('event_type', 'ORDER_UPDATE'),
                        "order_id": event_data.get('order_id'),
                        "timestamp": datetime.utcnow().isoformat(),
                        "data": event_data
                    }, event_data.get('order_id'))
                    
                    db.close()
                except Exception as e:
                    print(f"Error processing order event: {e}")

async def process_shipment_events(queue):
    """Process messages from the shipment events queue"""
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                try:
                    event_data = json.loads(message.body.decode())
                    print(f"Tracking Service: Processing shipment event for order {event_data.get('order_id')}")
                    
                    # Create tracking event
                    event = TrackingEvent(
                        event_id=f"evt-{uuid.uuid4().hex[:8]}",
                        order_id=event_data.get('order_id'),
                        shipment_id=event_data.get('shipment_id'),
                        event_type=event_data.get('event_type', 'SHIPMENT_UPDATE'),
                        event_data=event_data
                    )
                    
                    db = SessionLocal()
                    db.add(event)
                    db.commit()
                    
                    # Notify websocket clients
                    await manager.send_message({
                        "event_type": event_data.get('event_type', 'SHIPMENT_UPDATE'),
                        "order_id": event_data.get('order_id'),
                        "shipment_id": event_data.get('shipment_id'),
                        "timestamp": datetime.utcnow().isoformat(),
                        "data": event_data
                    }, event_data.get('order_id'))
                    
                    db.close()
                except Exception as e:
                    print(f"Error processing shipment event: {e}")

async def process_route_events(queue):
    """Process messages from the route events queue"""
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                try:
                    event_data = json.loads(message.body.decode())
                    print(f"Tracking Service: Processing route event for order {event_data.get('order_id')}")
                    
                    # Create tracking event
                    event = TrackingEvent(
                        event_id=f"evt-{uuid.uuid4().hex[:8]}",
                        order_id=event_data.get('order_id'),
                        route_id=event_data.get('route_id'),
                        event_type=event_data.get('event_type', 'ROUTE_UPDATE'),
                        status=event_data.get('status'),
                        location=event_data.get('location'),
                        description=event_data.get('description'),
                        updated_by=event_data.get('updated_by', 'system'),
                        event_data=event_data
                    )
                    
                    db = SessionLocal()
                    db.add(event)
                    db.commit()
                    
                    # Notify websocket clients
                    await manager.send_message({
                        "event_type": event_data.get('event_type', 'ROUTE_UPDATE'),
                        "order_id": event_data.get('order_id'),
                        "route_id": event_data.get('route_id'),
                        "status": event_data.get('status'),
                        "timestamp": datetime.utcnow().isoformat(),
                        "data": event_data
                    }, event_data.get('order_id'))
                    
                    db.close()
                except Exception as e:
                    print(f"Error processing route event: {e}")

async def process_status_updates(queue):
    """Process messages from the status update queue"""
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                try:
                    event_data = json.loads(message.body.decode())
                    print(f"Tracking Service: Processing status update for order {event_data.get('order_id')}")
                    
                    # Create tracking event for status update
                    event = TrackingEvent(
                        event_id=f"evt-{uuid.uuid4().hex[:8]}",
                        order_id=event_data.get('order_id'),
                        shipment_id=event_data.get('shipment_id'),
                        event_type='STATUS_UPDATE',
                        status=event_data.get('status'),
                        location=event_data.get('location'),
                        description=event_data.get('description'),
                        updated_by=event_data.get('driver_id', 'system'),
                        event_data=event_data
                    )
                    
                    db = SessionLocal()
                    db.add(event)
                    db.commit()
                    
                    # Notify websocket clients
                    await manager.send_message({
                        "event_type": 'STATUS_UPDATE',
                        "order_id": event_data.get('order_id'),
                        "status": event_data.get('status'),
                        "location": event_data.get('location'),
                        "description": event_data.get('description'),
                        "timestamp": datetime.utcnow().isoformat(),
                        "data": event_data
                    }, event_data.get('order_id'))
                    
                    db.close()
                except Exception as e:
                    print(f"Error processing status update: {e}")

async def publish_message(queue_name: str, message: dict):
    """Publish a message to a RabbitMQ queue"""
    try:
        if not hasattr(app.state, 'rabbitmq_channel') or app.state.rabbitmq_channel is None:
            print("Warning: RabbitMQ channel not available. Will retry connection.")
            # Try to reconnect
            await consume_messages()
            
        # Publish message
        message_body = json.dumps(message).encode()
        await app.state.rabbitmq_channel.default_exchange.publish(
            aio_pika.Message(body=message_body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=queue_name
        )
        print(f"Published message to {queue_name}: {message}")
        return True
    except Exception as e:
        print(f"Error publishing message to {queue_name}: {e}")
        return False

@app.websocket("/ws/track/{order_id}")
async def websocket_endpoint(websocket: WebSocket, order_id: str):
    """WebSocket endpoint for real-time tracking updates"""
    await manager.connect(websocket, order_id)
    
    try:
        # Send initial tracking events for this order
        db = SessionLocal()
        events = db.query(TrackingEvent).filter(TrackingEvent.order_id == order_id).order_by(TrackingEvent.timestamp).all()
        db.close()
        
        if events:
            # Send history of events
            await websocket.send_json({
                "type": "history",
                "order_id": order_id,
                "events": [
                    {
                        "event_id": event.event_id,
                        "event_type": event.event_type,
                        "timestamp": event.timestamp.isoformat(),
                        "data": event.event_data
                    } for event in events
                ]
            })
        
        # Wait for messages from the client (like ping or close)
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong", "timestamp": datetime.utcnow().isoformat()})
    except WebSocketDisconnect:
        manager.disconnect(websocket, order_id)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket, order_id)

@app.post("/tracking/events", response_model=TrackingEventResponse)
async def create_tracking_event(event: TrackingEventCreate, db=Depends(get_db)):
    """Manually create a tracking event via the API"""
    tracking_event = TrackingEvent(
        event_id=f"evt-{uuid.uuid4().hex[:8]}",
        order_id=event.order_id,
        shipment_id=event.shipment_id,
        route_id=event.route_id,
        event_type=event.event_type,
        event_data=event.event_data
    )
    
    db.add(tracking_event)
    db.commit()
    db.refresh(tracking_event)
    
    # Notify websocket clients
    await manager.send_message({
        "event_type": event.event_type,
        "order_id": event.order_id,
        "timestamp": tracking_event.timestamp.isoformat(),
        "data": event.event_data
    }, event.order_id)
    
    return TrackingEventResponse(
        event_id=tracking_event.event_id,
        order_id=tracking_event.order_id,
        shipment_id=tracking_event.shipment_id,
        route_id=tracking_event.route_id,
        event_type=tracking_event.event_type,
        event_data=tracking_event.event_data,
        timestamp=tracking_event.timestamp
    )

@app.get("/tracking/orders/{order_id}/events", response_model=List[TrackingEventResponse])
async def get_order_events(order_id: str, db=Depends(get_db)):
    """Get all tracking events for a specific order"""
    events = db.query(TrackingEvent).filter(TrackingEvent.order_id == order_id).order_by(TrackingEvent.timestamp).all()
    
    if not events:
        return []
    
    return [
        TrackingEventResponse(
            event_id=event.event_id,
            order_id=event.order_id,
            shipment_id=event.shipment_id,
            route_id=event.route_id,
            event_type=event.event_type,
            event_data=event.event_data,
            timestamp=event.timestamp
        ) for event in events
    ]

@app.get("/tracking/orders/{order_id}/latest", response_model=TrackingEventResponse)
async def get_latest_event(order_id: str, db=Depends(get_db)):
    """Get the latest tracking event for a specific order"""
    event = db.query(TrackingEvent).filter(TrackingEvent.order_id == order_id).order_by(TrackingEvent.timestamp.desc()).first()
    
    if not event:
        raise HTTPException(status_code=404, detail="No tracking events found for this order")
    
    return TrackingEventResponse(
        event_id=event.event_id,
        order_id=event.order_id,
        shipment_id=event.shipment_id,
        route_id=event.route_id,
        event_type=event.event_type,
        event_data=event.event_data,
        timestamp=event.timestamp
    )

@app.get("/tracking/health")
async def health_check():
    """Simple health check endpoint"""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}
