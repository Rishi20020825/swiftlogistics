# wms_mock/main.py - A proprietary WMS system mock
import asyncio
import random
import socket
import struct
import threading
from typing import Dict, List, Optional, Any
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta

app = FastAPI(title="WMS Mock Service")

# In-memory storage
inventory = {}
shipments = {}
warehouses = [
    {"id": "WH001", "name": "Central Warehouse", "location": "123 Logistics Way"},
    {"id": "WH002", "name": "North Distribution Center", "location": "456 Supply Chain Road"},
    {"id": "WH003", "name": "East Fulfillment Hub", "location": "789 Inventory Street"}
]

# Generate some initial inventory
for i in range(1, 100):
    sku = f"SKU-{i:04d}"
    inventory[sku] = {
        "sku": sku,
        "name": f"Product {i}",
        "description": f"This is product {i} - a sample product",
        "quantity": random.randint(10, 200),
        "location": f"AISLE-{random.randint(1, 20)}-SHELF-{random.randint(1, 50)}",
        "warehouse_id": random.choice(warehouses)["id"]
    }

# Pydantic models
class InventoryItem(BaseModel):
    sku: str
    name: str
    description: str
    quantity: int
    location: str
    warehouse_id: str

class ShipmentItem(BaseModel):
    sku: str
    quantity: int

class ShipmentRequest(BaseModel):
    order_id: str
    items: List[ShipmentItem]
    shipping_address: str

class ShipmentResponse(BaseModel):
    shipment_id: str
    order_id: str
    status: str = "PENDING"
    items: List[Dict[str, Any]]
    tracking_number: str
    estimated_delivery: str
    warehouse_id: str

# TCP Socket Server to simulate proprietary WMS protocol
class WMSSocketServer:
    def __init__(self, host='0.0.0.0', port=9876):
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = False
        self.thread = None
    
    def start(self):
        """Start the WMS socket server in a separate thread"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run_server)
        self.thread.daemon = True  # Set as daemon so it exits when main thread exits
        self.thread.start()
        print(f"WMS Socket Server started on {self.host}:{self.port}")
    
    def stop(self):
        """Stop the server"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        if self.thread:
            self.thread.join(timeout=1.0)
    
    def _run_server(self):
        """Run the server loop"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.server_socket.settimeout(1.0)  # 1 second timeout for accepting connections
            
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    print(f"WMS Socket: Connection from {address}")
                    self._handle_client(client_socket)
                except socket.timeout:
                    continue  # No connection within timeout, check if still running
                except Exception as e:
                    print(f"WMS Socket: Error accepting connection: {e}")
        
        except Exception as e:
            print(f"WMS Socket: Server error: {e}")
        finally:
            if self.server_socket:
                self.server_socket.close()
    
    def _handle_client(self, client_socket):
        """Handle a client connection in a separate thread"""
        thread = threading.Thread(target=self._client_thread, args=(client_socket,))
        thread.daemon = True
        thread.start()
    
    def _client_thread(self, client_socket):
        """Handle the client communication"""
        try:
            # First 4 bytes is message length as uint32
            length_bytes = client_socket.recv(4)
            if not length_bytes:
                return
            
            message_length = struct.unpack('!I', length_bytes)[0]
            
            # Then receive the message data
            message_data = b''
            bytes_received = 0
            
            while bytes_received < message_length:
                chunk = client_socket.recv(min(1024, message_length - bytes_received))
                if not chunk:
                    break
                message_data += chunk
                bytes_received += len(chunk)
            
            # Process the message (in a real system this would decode a proprietary format)
            if message_data:
                # Mock implementation - just read the command type from the first 4 bytes
                command = message_data[:4].decode('ascii')
                
                # Process different commands
                if command == 'INVQ':  # Inventory query
                    response = self._handle_inventory_query(message_data[4:])
                elif command == 'SHIP':  # Create shipment
                    response = self._handle_shipment_create(message_data[4:])
                elif command == 'STAT':  # Status update
                    response = self._handle_status_update(message_data[4:])
                else:
                    response = b'ERR:Unknown command'
                
                # Send response length followed by response
                response_length = len(response)
                client_socket.sendall(struct.pack('!I', response_length))
                client_socket.sendall(response)
        
        except Exception as e:
            print(f"WMS Socket: Error handling client: {e}")
        finally:
            client_socket.close()
    
    def _handle_inventory_query(self, data):
        """Handle an inventory query message"""
        try:
            # In a real system, this would parse a proprietary binary format
            # For this mock, we'll assume the data is just the SKU as ASCII
            sku = data.decode('ascii').strip()
            
            if sku in inventory:
                item = inventory[sku]
                # Format: "OK:quantity:location:warehouse_id"
                return f"OK:{item['quantity']}:{item['location']}:{item['warehouse_id']}".encode('ascii')
            else:
                return b'ERR:Item not found'
        except Exception as e:
            print(f"WMS Socket: Error handling inventory query: {e}")
            return b'ERR:Internal error'
    
    def _handle_shipment_create(self, data):
        """Handle a create shipment message"""
        try:
            # In a real system, this would parse a proprietary binary format
            # For simplicity, we'll assume a simple format: "order_id|item1:qty1,item2:qty2|address"
            parts = data.decode('ascii').split('|')
            if len(parts) != 3:
                return b'ERR:Invalid format'
            
            order_id = parts[0]
            items_str = parts[1]
            address = parts[2]
            
            items = []
            for item_str in items_str.split(','):
                if not item_str:
                    continue
                sku, qty = item_str.split(':')
                if sku in inventory and inventory[sku]['quantity'] >= int(qty):
                    items.append({
                        "sku": sku,
                        "quantity": int(qty)
                    })
            
            if not items:
                return b'ERR:No valid items'
            
            # Create a shipment
            shipment_id = f"SHP{random.randint(10000, 99999)}"
            tracking = f"TRK{random.randint(1000000, 9999999)}"
            warehouse_id = random.choice(warehouses)["id"]
            
            shipments[shipment_id] = {
                "shipment_id": shipment_id,
                "order_id": order_id,
                "status": "PENDING",
                "items": items,
                "tracking_number": tracking,
                "shipping_address": address,
                "warehouse_id": warehouse_id,
                "estimated_delivery": (datetime.utcnow() + timedelta(days=random.randint(1, 5))).strftime("%Y-%m-%d")
            }
            
            # Format: "OK:shipment_id:tracking_number:estimated_delivery:warehouse_id"
            return f"OK:{shipment_id}:{tracking}:{shipments[shipment_id]['estimated_delivery']}:{warehouse_id}".encode('ascii')
        
        except Exception as e:
            print(f"WMS Socket: Error handling shipment create: {e}")
            return b'ERR:Internal error'
    
    def _handle_status_update(self, data):
        """Handle a status update message"""
        try:
            # Format: "shipment_id|new_status"
            parts = data.decode('ascii').split('|')
            if len(parts) != 2:
                return b'ERR:Invalid format'
            
            shipment_id = parts[0]
            new_status = parts[1]
            
            if shipment_id not in shipments:
                return b'ERR:Shipment not found'
            
            valid_statuses = ["PENDING", "PICKING", "PACKED", "SHIPPED", "DELIVERED"]
            if new_status not in valid_statuses:
                return b'ERR:Invalid status'
            
            shipments[shipment_id]["status"] = new_status
            return b'OK:Status updated'
        
        except Exception as e:
            print(f"WMS Socket: Error handling status update: {e}")
            return b'ERR:Internal error'

# Initialize the socket server
wms_socket_server = WMSSocketServer()

@app.on_event("startup")
async def startup():
    """Start the WMS socket server when the FastAPI app starts"""
    wms_socket_server.start()

@app.on_event("shutdown")
async def shutdown():
    """Stop the WMS socket server when the FastAPI app stops"""
    wms_socket_server.stop()

@app.get("/inventory/{sku}")
async def get_inventory(sku: str):
    """REST API to check inventory (alternative to socket protocol)"""
    if sku not in inventory:
        raise HTTPException(status_code=404, detail="Item not found")
    return inventory[sku]

@app.post("/shipments", response_model=ShipmentResponse)
async def create_shipment(shipment_request: ShipmentRequest):
    """REST API to create a shipment (alternative to socket protocol)"""
    # Validate items are in stock
    items = []
    for item in shipment_request.items:
        if item.sku not in inventory:
            raise HTTPException(status_code=404, detail=f"Item {item.sku} not found")
        
        if inventory[item.sku]["quantity"] < item.quantity:
            raise HTTPException(status_code=400, detail=f"Insufficient quantity for {item.sku}")
        
        items.append({
            "sku": item.sku,
            "quantity": item.quantity,
            "name": inventory[item.sku]["name"],
            "description": inventory[item.sku]["description"],
            "location": inventory[item.sku]["location"]
        })
    
    # Create a shipment
    shipment_id = f"SHP{random.randint(10000, 99999)}"
    tracking = f"TRK{random.randint(1000000, 9999999)}"
    warehouse = random.choice(warehouses)
    
    shipment = {
        "shipment_id": shipment_id,
        "order_id": shipment_request.order_id,
        "status": "PENDING",
        "items": items,
        "tracking_number": tracking,
        "shipping_address": shipment_request.shipping_address,
        "warehouse_id": warehouse["id"],
        "estimated_delivery": (datetime.utcnow() + timedelta(days=random.randint(1, 5))).strftime("%Y-%m-%d")
    }
    
    shipments[shipment_id] = shipment
    
    # Reduce inventory
    for item in shipment_request.items:
        inventory[item.sku]["quantity"] -= item.quantity
    
    return ShipmentResponse(
        shipment_id=shipment_id,
        order_id=shipment_request.order_id,
        status="PENDING",
        items=items,
        tracking_number=tracking,
        estimated_delivery=shipment["estimated_delivery"],
        warehouse_id=warehouse["id"]
    )

@app.get("/shipments/{shipment_id}")
async def get_shipment(shipment_id: str):
    """Get a shipment by ID"""
    if shipment_id not in shipments:
        raise HTTPException(status_code=404, detail="Shipment not found")
    return shipments[shipment_id]

@app.put("/shipments/{shipment_id}/status")
async def update_shipment_status(shipment_id: str, status: str):
    """Update a shipment's status"""
    if shipment_id not in shipments:
        raise HTTPException(status_code=404, detail="Shipment not found")
    
    valid_statuses = ["PENDING", "PICKING", "PACKED", "SHIPPED", "DELIVERED"]
    if status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    shipments[shipment_id]["status"] = status
    return {"message": f"Shipment status updated to {status}", "shipment_id": shipment_id}

@app.get("/warehouses")
async def get_warehouses():
    """Get all warehouses"""
    return warehouses

@app.get("/warehouses/{warehouse_id}")
async def get_warehouse(warehouse_id: str):
    """Get a warehouse by ID"""
    warehouse = next((w for w in warehouses if w["id"] == warehouse_id), None)
    if not warehouse:
        raise HTTPException(status_code=404, detail="Warehouse not found")
    return warehouse
