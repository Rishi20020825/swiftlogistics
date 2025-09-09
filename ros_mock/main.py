from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import random
import uuid

app = FastAPI(title="ROS Mock Service")

# In-memory storage for routes
routes = {}

class DeliveryPoint(BaseModel):
    address: str
    lat: Optional[float] = None
    lng: Optional[float] = None
    priority: Optional[int] = 1  # 1 is highest priority

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

class Route(BaseModel):
    route_id: str
    order_id: str
    vehicle_id: str
    segments: List[RouteSegment]
    total_distance_km: float
    total_time_minutes: int
    status: str = "PLANNED"  # PLANNED, ACTIVE, COMPLETED, FAILED

@app.post("/routes", response_model=Route)
async def create_route(route_request: RouteRequest):
    """Generate an optimized route for the delivery points"""
    # This is a mock - in a real system, it would use sophisticated algorithms
    # to determine the optimal route based on various constraints
    
    # Generate a random route for demo purposes
    route_id = f"route-{uuid.uuid4().hex[:8]}"
    segments = []
    total_distance = 0
    total_time = 0
    
    # Assign a random vehicle
    if not route_request.vehicles:
        raise HTTPException(status_code=400, detail="At least one vehicle must be provided")
    
    vehicle = random.choice(route_request.vehicles)
    
    # Create route segments between delivery points
    points = route_request.delivery_points
    for i in range(len(points) - 1):
        # Create a mock segment
        distance = random.uniform(2.0, 15.0)  # Random distance in km
        time = int(distance * 3)  # Roughly 3 minutes per km
        
        segment = RouteSegment(
            start_address=points[i].address,
            end_address=points[i+1].address,
            distance_km=round(distance, 1),
            estimated_time_minutes=time
        )
        segments.append(segment)
        total_distance += distance
        total_time += time
    
    # Create the route
    route = Route(
        route_id=route_id,
        order_id=route_request.order_id,
        vehicle_id=vehicle.id,
        segments=segments,
        total_distance_km=round(total_distance, 1),
        total_time_minutes=total_time,
        status="PLANNED"
    )
    
    # Save the route
    routes[route_id] = route
    
    return route

@app.get("/routes/{route_id}", response_model=Route)
async def get_route(route_id: str):
    """Get a specific route by ID"""
    if route_id not in routes:
        raise HTTPException(status_code=404, detail="Route not found")
    return routes[route_id]

@app.get("/routes/by-order/{order_id}", response_model=List[Route])
async def get_routes_by_order(order_id: str):
    """Get all routes for a specific order"""
    order_routes = [r for r in routes.values() if r.order_id == order_id]
    return order_routes

@app.put("/routes/{route_id}/status")
async def update_route_status(route_id: str, status: str):
    """Update the status of a route"""
    if route_id not in routes:
        raise HTTPException(status_code=404, detail="Route not found")
        
    valid_statuses = ["PLANNED", "ACTIVE", "COMPLETED", "FAILED"]
    if status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    routes[route_id].status = status
    return {"message": f"Route status updated to {status}", "route_id": route_id}
