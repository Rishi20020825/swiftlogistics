# swiftlogistics/ros_adapter/main.py
import os
import asyncio
import json
import aio_pika
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import create_engine, Column, Integer, String, Float, JSON
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Route(Base):
    __tablename__ = "routes"
    id = Column(Integer, primary_key=True, index=True)
    route_id = Column(String, unique=True, index=True)
    order_id = Column(String, index=True)
    vehicle_id = Column(String)
    total_distance_km = Column(Float)
    total_time_minutes = Column(Integer)
    segments = Column(JSON)
    status = Column(String, default="PLANNED")

# Initialize database
Base.metadata.create_all(bind=engine)

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for API
class DeliveryPoint(BaseModel):
    address: str
    lat: Optional[float] = None
    lng: Optional[float] = None
    priority: Optional[int] = 1

class Vehicle(BaseModel):
    id: str
    capacity: int
    current_location: Optional[str] = None

class RouteRequest(BaseModel):
    order_id: str
    delivery_points: List[DeliveryPoint]
    vehicles: List[Vehicle]

class RouteSegment(BaseModel):
    start_address: str
    end_address: str
    distance_km: float
    estimated_time_minutes: int

class RouteResponse(BaseModel):
    route_id: str
    order_id: str
    vehicle_id: str
    segments: List[RouteSegment]
    total_distance_km: float
    total_time_minutes: int
    status: str

async def consume_messages():
    try:
        rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
        print(f"Connecting to RabbitMQ at: {rabbitmq_url}")
        connection = await aio_pika.connect_robust(rabbitmq_url)
        channel = await connection.channel()
        queue = await channel.declare_queue("order_queue", durable=True)
        
        print("Successfully connected to RabbitMQ")
        
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    order_data = json.loads(message.body.decode())
                    print(f"ROS Adapter: Processing order {order_data['order_id']}")
                    
                    # Process the order through ROS mock service
                    await process_order_for_routing(order_data)
    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")
        print("Will retry in 10 seconds...")
        await asyncio.sleep(10)
        asyncio.create_task(consume_messages())  # Retry connecting

async def process_order_for_routing(order_data):
    """Process an order by creating a route through the ROS mock service"""
    try:
        # Prepare route request
        delivery_points = [
            DeliveryPoint(address=order_data.get('address', 'Unknown Address'))
        ]
        
        # Add a fake destination for demonstration
        delivery_points.append(
            DeliveryPoint(
                address="Central Warehouse, 123 Logistics Way",
                priority=2
            )
        )
        
        # Create vehicles (in a real system, would come from a vehicle management service)
        vehicles = [
            Vehicle(id="van-001", capacity=10),
            Vehicle(id="truck-002", capacity=20)
        ]
        
        # Create route request
        route_request = {
            "order_id": str(order_data.get('order_id', 'unknown')),
            "delivery_points": [point.dict() for point in delivery_points],
            "vehicles": [vehicle.dict() for vehicle in vehicles]
        }
        
        # Call ROS mock service
        ros_mock_url = os.getenv("ROS_MOCK_URL", "http://ros_mock:8000/routes")
        async with httpx.AsyncClient() as client:
            response = await client.post(
                ros_mock_url,
                json=route_request,
                timeout=10.0
            )
            
            if response.status_code == 200:
                route_data = response.json()
                print(f"ROS Adapter: Route created with ID {route_data['route_id']} for order {order_data.get('order_id')}")
                
                # Store the route in the database
                db = SessionLocal()
                route = Route(
                    route_id=route_data['route_id'],
                    order_id=route_data['order_id'],
                    vehicle_id=route_data['vehicle_id'],
                    total_distance_km=route_data['total_distance_km'],
                    total_time_minutes=route_data['total_time_minutes'],
                    segments=route_data['segments'],
                    status=route_data['status']
                )
                db.add(route)
                db.commit()
                db.close()
                
                print(f"ROS Adapter: Route saved to database")
                
                # In a real system, you would then publish a message to a 'route_created' queue
                # for other services to pick up
                
                return route_data
            else:
                print(f"ROS Adapter: Failed to create route. Status: {response.status_code}")
                print(f"Response: {response.text}")
                return None
    except Exception as e:
        print(f"ROS Adapter: Error processing order for routing: {e}")
        return None

@app.get("/ros/routes/{route_id}")
async def get_route(route_id: str):
    """Get route details by route ID"""
    db = SessionLocal()
    route = db.query(Route).filter(Route.route_id == route_id).first()
    db.close()
    
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")
    
    return {
        "route_id": route.route_id,
        "order_id": route.order_id,
        "vehicle_id": route.vehicle_id,
        "total_distance_km": route.total_distance_km,
        "total_time_minutes": route.total_time_minutes,
        "segments": route.segments,
        "status": route.status
    }

@app.get("/ros/routes/by-order/{order_id}")
async def get_routes_by_order(order_id: str):
    """Get all routes for a specific order"""
    db = SessionLocal()
    routes = db.query(Route).filter(Route.order_id == order_id).all()
    db.close()
    
    if not routes:
        return []
    
    return [
        {
            "route_id": route.route_id,
            "order_id": route.order_id,
            "vehicle_id": route.vehicle_id,
            "total_distance_km": route.total_distance_km,
            "total_time_minutes": route.total_time_minutes,
            "segments": route.segments,
            "status": route.status
        }
        for route in routes
    ]

@app.post("/ros/create-route")
async def create_route(route_request: RouteRequest):
    """Create a new route through the ROS service"""
    ros_mock_url = os.getenv("ROS_MOCK_URL", "http://ros_mock:8000/routes")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                ros_mock_url,
                json=route_request.dict(),
                timeout=10.0
            )
            
            if response.status_code == 200:
                route_data = response.json()
                
                # Store the route in the database
                db = SessionLocal()
                route = Route(
                    route_id=route_data['route_id'],
                    order_id=route_data['order_id'],
                    vehicle_id=route_data['vehicle_id'],
                    total_distance_km=route_data['total_distance_km'],
                    total_time_minutes=route_data['total_time_minutes'],
                    segments=route_data['segments'],
                    status=route_data['status']
                )
                db.add(route)
                db.commit()
                db.close()
                
                return route_data
            else:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Failed to create route: {response.text}"
                )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating route: {str(e)}")

@app.put("/ros/routes/{route_id}/status")
async def update_route_status(route_id: str, status: str):
    """Update the status of a route"""
    # Validate status
    valid_statuses = ["PLANNED", "ACTIVE", "COMPLETED", "FAILED"]
    if status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    # Update in database
    db = SessionLocal()
    route = db.query(Route).filter(Route.route_id == route_id).first()
    
    if not route:
        db.close()
        raise HTTPException(status_code=404, detail="Route not found")
    
    # Update in database
    route.status = status
    db.commit()
    
    # Also update in ROS mock service
    try:
        ros_mock_url = os.getenv("ROS_MOCK_URL", f"http://ros_mock:8000/routes/{route_id}/status")
        async with httpx.AsyncClient() as client:
            await client.put(
                ros_mock_url,
                params={"status": status},
                timeout=10.0
            )
    except Exception as e:
        print(f"Warning: Failed to update status in ROS mock: {e}")
    finally:
        db.close()
    
    return {"message": f"Route status updated to {status}", "route_id": route_id}

@app.on_event("startup")
async def startup_event():
    # Create database tables if they don't exist
    Base.metadata.create_all(bind=engine)
    # Start consuming messages from RabbitMQ
    asyncio.create_task(consume_messages())