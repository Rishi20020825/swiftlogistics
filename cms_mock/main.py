
from fastapi import FastAPI, Request, Response
import xml.etree.ElementTree as ET

app = FastAPI()

# In-memory order store
orders = []

def parse_soap_order(xml_body):
    # Very basic XML parsing for SOAP envelope
    try:
        root = ET.fromstring(xml_body)
        # Find order fields in SOAP body (assume structure for prototype)
        ns = {'soap': 'http://schemas.xmlsoap.org/soap/envelope/'}
        body = root.find('soap:Body', ns)
        
        # Check if this is a GetOrdersRequest
        if body.find('GetOrdersRequest') is not None:
            return {'request_type': 'get_orders'}
            
        # Otherwise parse as regular order
        order = body.find('Order') if body is not None else None
        if order is not None:
            client = order.findtext('Client')
            order_id = order.findtext('OrderID')
            items = [item.text for item in order.findall('Items/Item')]
            address = order.findtext('Address')
            return {
                'request_type': 'create_order',
                'client': client,
                'order_id': order_id,
                'items': items,
                'address': address
            }
    except Exception as e:
        print(f"SOAP parse error: {e}")
    return None

def make_soap_response(order_id):
    # Simple SOAP XML response
    return f'''<?xml version="1.0"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <OrderResponse>
          <OrderID>{order_id}</OrderID>
          <Status>Received</Status>
        </OrderResponse>
      </soap:Body>
    </soap:Envelope>'''

def make_orders_response(orders_list):
    # Generate SOAP response with all orders
    orders_xml = ""
    for order in orders_list:
        items_xml = ""
        for item in order.get('items', []):
            items_xml += f"<Item>{item}</Item>"
            
        orders_xml += f'''
        <Order>
          <OrderID>{order.get('order_id', '')}</OrderID>
          <Client>{order.get('client', '')}</Client>
          <Status>Processed</Status>
          <Address>{order.get('address', '')}</Address>
          <Items>{items_xml}</Items>
        </Order>'''
        
    return f'''<?xml version="1.0"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <OrdersResponse>
          {orders_xml}
        </OrdersResponse>
      </soap:Body>
    </soap:Envelope>'''

@app.post("/orders")
async def process_orders(request: Request):
    content_type = request.headers.get("content-type", "")
    if "xml" in content_type:
        xml_body = await request.body()
        parsed_data = parse_soap_order(xml_body.decode())
        
        if not parsed_data:
            return Response(content="<error>Invalid SOAP</error>", media_type="application/xml", status_code=400)
        
        # Handle different request types
        if parsed_data.get('request_type') == 'get_orders':
            # Return all orders
            soap_resp = make_orders_response(orders)
            return Response(content=soap_resp, media_type="application/xml")
            
        elif parsed_data.get('request_type') == 'create_order':
            # Process new order
            order_data = {
                'client': parsed_data.get('client'),
                'order_id': parsed_data.get('order_id'),
                'items': parsed_data.get('items'),
                'address': parsed_data.get('address')
            }
            orders.append(order_data)
            soap_resp = make_soap_response(order_data['order_id'])
            return Response(content=soap_resp, media_type="application/xml")
            
    return Response(content="<error>Unsupported Media Type</error>", media_type="application/xml", status_code=415)
