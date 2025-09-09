# swiftlogistics/wms_adapter/main.py
import os
import asyncio
import json
import aio_pika
import socket
import time
import random
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class InventoryItem(Base):
    __tablename__ = "inventory_items"
    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String, unique=True, index=True)
    name = Column(String)
    description = Column(String)
    quantity = Column(Integer, default=0)
    location = Column(String)
    last_updated = Column(DateTime, default=datetime.utcnow)

class Shipment(Base):
    __tablename__ = "shipments"
    id = Column(Integer, primary_key=True, index=True)
    shipment_id = Column(String, unique=True, index=True)
    order_id = Column(String, index=True)
    status = Column(String, default="PENDING")  # PENDING, PICKING, PACKED, SHIPPED
    items = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    tracking_number = Column(String, nullable=True)
    weight_kg = Column(Float, nullable=True)

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
class ItemBase(BaseModel):
    sku: str
    quantity: int

class OrderItem(ItemBase):
    name: Optional[str] = None
    description: Optional[str] = None

class ShipmentRequest(BaseModel):
    order_id: str
    items: List[OrderItem]
    shipping_address: str

class ShipmentResponse(BaseModel):
    shipment_id: str
    order_id: str
    status: str
    items: List[Dict[str, Any]]
    tracking_number: Optional[str] = None
    estimated_delivery: Optional[str] = None

# Mock WMS TCP/IP Protocol Handler
class WMSProtocolHandler:
    """
    Simulates a proprietary TCP/IP protocol for a legacy WMS system.
    In a real implementation, this would use specific byte formats, checksums,
    and handle various error cases.
    """
    
    @staticmethod
    async def send_command(command: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate sending a command to the WMS system using TCP/IP socket"""
        try:
            # Get connection details from environment
            host = os.getenv("WMS_MOCK_TCP_HOST", "wms_mock")
            port = int(os.getenv("WMS_MOCK_TCP_PORT", "9876"))
            
            # Connect to the WMS system via TCP socket
            reader, writer = await asyncio.open_connection(host, port)
            
            # Format the message based on the command type
            if command == "CREATE_SHIPMENT":
                # Format: "SHIP|order_id|item1:qty1,item2:qty2,...|shipping_address"
                items_str = ",".join([f"{item.get('sku')}:{item.get('quantity')}" for item in data.get("items", [])])
                message = f"SHIP|{data.get('order_id')}|{items_str}|{data.get('shipping_address')}"
            elif command == "CHECK_INVENTORY":
                # Format: "INVQ|sku"
                message = f"INVQ|{data.get('sku')}"
            elif command == "UPDATE_SHIPMENT_STATUS":
                # Format: "STAT|shipment_id|new_status"
                message = f"STAT|{data.get('shipment_id')}|{data.get('new_status', 'UNKNOWN')}"
            else:
                return {
                    "status": "ERROR",
                    "error_code": "UNKNOWN_COMMAND",
                    "message": f"Command {command} not recognized"
                }
            
            # Send the message length (4 bytes) followed by the message
            message_bytes = message.encode('ascii')
            message_length = len(message_bytes)
            writer.write(message_length.to_bytes(4, byteorder='big'))
            writer.write(message_bytes)
            await writer.drain()
            
            # Read the response length (4 bytes)
            length_bytes = await reader.read(4)
            response_length = int.from_bytes(length_bytes, byteorder='big')
            
            # Read the response
            response_bytes = await reader.read(response_length)
            response_text = response_bytes.decode('ascii')
            
            # Close the connection
            writer.close()
            await writer.wait_closed()
            
            # Parse the response
            if response_text.startswith("OK:"):
                parts = response_text[3:].split(":")
                
                if command == "CREATE_SHIPMENT":
                    # Format: "OK:shipment_id:tracking_number:estimated_delivery:warehouse_id"
                    return {
                        "status": "SUCCESS",
                        "shipment_id": parts[0],
                        "tracking_number": parts[1],
                        "estimated_delivery": parts[2],
                        "warehouse_id": parts[3]
                    }
                elif command == "CHECK_INVENTORY":
                    # Format: "OK:quantity:location:warehouse_id"
                    return {
                        "status": "SUCCESS",
                        "sku": data.get("sku"),
                        "in_stock": int(parts[0]) > 0,
                        "quantity": int(parts[0]),
                        "location": parts[1],
                        "warehouse_id": parts[2]
                    }
                elif command == "UPDATE_SHIPMENT_STATUS":
                    # Format: "OK:Status updated"
                    return {
                        "status": "SUCCESS",
                        "shipment_id": data.get("shipment_id"),
                        "new_status": data.get("new_status")
                    }
            else:
                # Error response
                error_message = response_text[4:] if response_text.startswith("ERR:") else "Unknown error"
                return {
                    "status": "ERROR",
                    "error_code": "WMS_ERROR",
                    "message": error_message
                }
            
        except Exception as e:
            print(f"WMS Protocol Error: {e}")
            return {
                "status": "ERROR",
                "error_code": "CONNECTION_ERROR",
                "message": f"Failed to communicate with WMS system: {str(e)}"
            }

async def consume_messages():
    try:
        rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
        print(f"WMS Adapter: Connecting to RabbitMQ at: {rabbitmq_url}")
        connection = await aio_pika.connect_robust(rabbitmq_url)
        channel = await connection.channel()
        queue = await channel.declare_queue("order_queue", durable=True)
        
        print("WMS Adapter: Successfully connected to RabbitMQ")
        
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    order_data = json.loads(message.body.decode())
                    print(f"WMS Adapter: Processing order {order_data['order_id']}")
                    
                    # Create a shipment for the order
                    await process_order_for_shipment(order_data)
    except Exception as e:
        print(f"WMS Adapter: Error connecting to RabbitMQ: {e}")
        print("Will retry in 10 seconds...")
        await asyncio.sleep(10)
        asyncio.create_task(consume_messages())  # Retry connecting

async def process_order_for_shipment(order_data):
    """Process an order by creating a shipment through the WMS protocol"""
    try:
        wms_protocol = WMSProtocolHandler()
        
        # Check inventory for items
        items = []
        for item in order_data.get('items', []):
            inventory_check = await wms_protocol.send_command("CHECK_INVENTORY", {
                "sku": item.get('sku', f"SKU-{random.randint(1000, 9999)}"),
            })
            
            if inventory_check.get('in_stock', False):
                items.append({
                    "sku": inventory_check.get('sku'),
                    "quantity": item.get('quantity', 1),
                    "location": inventory_check.get('location')
                })
            else:
                print(f"WMS Adapter: Warning - Item {item.get('sku')} not in stock")
        
        # Create shipment
        shipment_data = await wms_protocol.send_command("CREATE_SHIPMENT", {
            "order_id": order_data.get('order_id'),
            "items": items,
            "shipping_address": order_data.get('address', 'No address provided')
        })
        
        if shipment_data.get('status') == "SUCCESS":
            # Store the shipment in the database
            db = SessionLocal()
            shipment = Shipment(
                shipment_id=shipment_data['shipment_id'],
                order_id=order_data.get('order_id'),
                status="PENDING",
                items=items,
                tracking_number=shipment_data.get('tracking_number')
            )
            db.add(shipment)
            db.commit()
            db.close()
            
            print(f"WMS Adapter: Shipment created with ID {shipment_data['shipment_id']} for order {order_data.get('order_id')}")
            return shipment_data
        else:
            print(f"WMS Adapter: Failed to create shipment: {shipment_data.get('message', 'Unknown error')}")
            return None
    except Exception as e:
        print(f"WMS Adapter: Error processing order for shipment: {e}")
        return None

@app.get("/wms/shipments/{shipment_id}")
async def get_shipment(shipment_id: str):
    """Get shipment details by shipment ID"""
    db = SessionLocal()
    shipment = db.query(Shipment).filter(Shipment.shipment_id == shipment_id).first()
    db.close()
    
    if not shipment:
        raise HTTPException(status_code=404, detail="Shipment not found")
    
    return {
        "shipment_id": shipment.shipment_id,
        "order_id": shipment.order_id,
        "status": shipment.status,
        "items": shipment.items,
        "created_at": shipment.created_at,
        "updated_at": shipment.updated_at,
        "tracking_number": shipment.tracking_number,
        "weight_kg": shipment.weight_kg
    }

@app.get("/wms/shipments/by-order/{order_id}")
async def get_shipments_by_order(order_id: str):
    """Get all shipments for a specific order"""
    db = SessionLocal()
    shipments = db.query(Shipment).filter(Shipment.order_id == order_id).all()
    db.close()
    
    if not shipments:
        return []
    
    return [
        {
            "shipment_id": shipment.shipment_id,
            "order_id": shipment.order_id,
            "status": shipment.status,
            "items": shipment.items,
            "created_at": shipment.created_at,
            "updated_at": shipment.updated_at,
            "tracking_number": shipment.tracking_number,
            "weight_kg": shipment.weight_kg
        }
        for shipment in shipments
    ]

@app.put("/wms/shipments/{shipment_id}/status")
async def update_shipment_status(shipment_id: str, status: str):
    """Update the status of a shipment"""
    # Validate status
    valid_statuses = ["PENDING", "PICKING", "PACKED", "SHIPPED", "DELIVERED"]
    if status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    # Update in database
    db = SessionLocal()
    shipment = db.query(Shipment).filter(Shipment.shipment_id == shipment_id).first()
    
    if not shipment:
        db.close()
        raise HTTPException(status_code=404, detail="Shipment not found")
    
    # Update in database
    shipment.status = status
    shipment.updated_at = datetime.utcnow()
    db.commit()
    
    # Also update in WMS system
    try:
        wms_protocol = WMSProtocolHandler()
        await wms_protocol.send_command("UPDATE_SHIPMENT_STATUS", {
            "shipment_id": shipment_id,
            "new_status": status
        })
    except Exception as e:
        print(f"Warning: Failed to update status in WMS: {e}")
    finally:
        db.close()
    
    return {"message": f"Shipment status updated to {status}", "shipment_id": shipment_id}

@app.post("/wms/create-shipment")
async def create_shipment(shipment_request: ShipmentRequest):
    """Create a new shipment through the WMS system"""
    try:
        wms_protocol = WMSProtocolHandler()
        
        items = []
        for item in shipment_request.items:
            inventory_check = await wms_protocol.send_command("CHECK_INVENTORY", {
                "sku": item.sku
            })
            
            if inventory_check.get('in_stock', False):
                items.append({
                    "sku": item.sku,
                    "quantity": item.quantity,
                    "name": item.name,
                    "description": item.description,
                    "location": inventory_check.get('location')
                })
            else:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Item {item.sku} not in stock"
                )
        
        # Create shipment
        shipment_data = await wms_protocol.send_command("CREATE_SHIPMENT", {
            "order_id": shipment_request.order_id,
            "items": items,
            "shipping_address": shipment_request.shipping_address
        })
        
        if shipment_data.get('status') == "SUCCESS":
            # Store the shipment in the database
            db = SessionLocal()
            shipment = Shipment(
                shipment_id=shipment_data['shipment_id'],
                order_id=shipment_request.order_id,
                status="PENDING",
                items=items,
                tracking_number=shipment_data.get('tracking_number')
            )
            db.add(shipment)
            db.commit()
            db.close()
            
            return {
                "shipment_id": shipment_data['shipment_id'],
                "order_id": shipment_request.order_id,
                "status": "PENDING",
                "items": items,
                "tracking_number": shipment_data.get('tracking_number'),
                "estimated_delivery": shipment_data.get('estimated_delivery')
            }
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create shipment: {shipment_data.get('message', 'Unknown error')}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating shipment: {str(e)}")

@app.on_event("startup")
async def startup_event():
    # Create database tables if they don't exist
    Base.metadata.create_all(bind=engine)
    # Start consuming messages from RabbitMQ
    asyncio.create_task(consume_messages())