# swiftlogistics/cms_adapter/main.py
import os
import asyncio
import json
import aio_pika
import xml.etree.ElementTree as ET
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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


from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pika

app = FastAPI()

# Add CORS middleware to allow frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For production, specify your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

## Removed mistakenly added CMS mock endpoints. This file should only contain adapter logic.


import httpx

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
                    print(f"CMS Adapter: Processing order {order_data['order_id']}")

                    # --- Forward to CMS Mock Service (convert REST to SOAP/XML) ---
                    cms_mock_url = os.getenv("CMS_MOCK_URL", "http://cms_mock:8000/orders")
                    soap_xml = dict_to_soap(order_data)
                    try:
                        async with httpx.AsyncClient() as client:
                            response = await client.post(
                                cms_mock_url, 
                                content=soap_xml, 
                                headers={"Content-Type": "application/xml"}
                            )
                            if response.status_code == 200:
                                print(f"CMS Adapter: Order {order_data['order_id']} created in CMS Mock.")
                            else:
                                print(f"CMS Adapter: Failed to create order in CMS Mock. Status: {response.status_code}")
                    except Exception as e:
                        print(f"CMS Adapter: Error communicating with CMS Mock: {e}")

                    # Update the database to reflect the new status
                    db = SessionLocal()
                    order = db.query(Order).get(order_data['order_id'])
                    if order:
                        order.status = "CMS-confirmed"
                        db.commit()
                    db.close()
    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")
        print("Will retry in 10 seconds...")
        await asyncio.sleep(10)
        asyncio.create_task(consume_messages())  # Retry connecting# Helper functions for XML conversion
def dict_to_soap(order):
    items_xml = ''.join([f'<Item>{item}</Item>' for item in order.get('items', [])])
    return f'''<?xml version="1.0"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <Order>
          <Client>{order.get('client')}</Client>
          <OrderID>{order.get('order_id')}</OrderID>
          <Items>{items_xml}</Items>
          <Address>{order.get('address')}</Address>
        </Order>
      </soap:Body>
    </soap:Envelope>'''

def soap_to_dict(xml_string):
    try:
        root = ET.fromstring(xml_string)
        ns = {'soap': 'http://schemas.xmlsoap.org/soap/envelope/'}
        body = root.find('soap:Body', ns)
        
        # Handle different response types
        order_resp = body.find('OrderResponse')
        if order_resp is not None:
            return {
                'order_id': order_resp.findtext('OrderID'),
                'status': order_resp.findtext('Status')
            }
            
        orders_resp = body.find('OrdersResponse')
        if orders_resp is not None:
            result = []
            for order_elem in orders_resp.findall('Order'):
                order = {
                    'order_id': order_elem.findtext('OrderID'),
                    'client': order_elem.findtext('Client'),
                    'status': order_elem.findtext('Status'),
                    'address': order_elem.findtext('Address'),
                    'items': [item.text for item in order_elem.findall('Items/Item')]
                }
                result.append(order)
            return result
            
        return None
    except Exception as e:
        print(f"Error parsing SOAP response: {e}")
        return None

class OrderCreate(BaseModel):
    client: str
    order_id: str
    items: list
    address: str

@app.get("/cms/orders")
async def get_orders():
    """Get all orders from CMS (Frontend -> Adapter -> CMS Mock)"""
    cms_mock_url = os.getenv("CMS_MOCK_URL", "http://cms_mock:8000/orders")
    
    # Create SOAP request for getting orders
    soap_request = '''<?xml version="1.0"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <GetOrdersRequest/>
      </soap:Body>
    </soap:Envelope>'''
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                cms_mock_url, 
                content=soap_request, 
                headers={"Content-Type": "application/xml"}
            )
            
            if response.status_code == 200:
                # Convert SOAP response to JSON
                orders = soap_to_dict(response.text)
                if orders:
                    return orders
                # Fallback for demo
                return []
            else:
                # For demo, return empty if CMS is unavailable
                return []
    except Exception as e:
        print(f"Error getting orders from CMS: {e}")
        return []

@app.post("/cms/orders")
async def create_order(order: OrderCreate):
    """Create a new order (Frontend -> Adapter -> CMS Mock)"""
    cms_mock_url = os.getenv("CMS_MOCK_URL", "http://cms_mock:8000/orders")
    
    # Convert REST/JSON to SOAP/XML
    soap_xml = dict_to_soap(order.dict())
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                cms_mock_url, 
                content=soap_xml, 
                headers={"Content-Type": "application/xml"}
            )
            
            if response.status_code == 200:
                # Parse SOAP response and convert to JSON
                result = soap_to_dict(response.text)
                if result:
                    return result
                return {"status": "Order created", "order_id": order.order_id}
            else:
                raise HTTPException(status_code=response.status_code, detail="Failed to create order in CMS")
    except Exception as e:
        print(f"Error creating order in CMS: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing order: {str(e)}")

@app.on_event("startup")
async def startup_event():
    # Start the consumer as a background task to not block the main app
    asyncio.create_task(consume_messages())