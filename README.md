# SwiftLogistics Project

## System Architecture

SwiftLogistics is a comprehensive middleware system designed to connect heterogeneous systems in a logistics environment. It provides protocol conversion, distributed transaction management, real-time tracking, and integration with various logistics services.

### Core Services

- **Order Service**: Central service for managing orders
- **Transaction Service**: Manages distributed transactions with compensation-based rollback
- **Tracking Service**: Provides real-time order tracking via WebSockets

### Adapter Services

- **CMS Adapter**: Connects to a legacy Customer Management System using SOAP/XML
- **ROS Adapter**: Integrates with a Route Optimization System using REST/JSON
- **WMS Adapter**: Interfaces with a Warehouse Management System using proprietary TCP/IP protocol

### Mock Services (for development/testing)

- **CMS Mock**: Simulates a legacy CMS with SOAP/XML interface
- **ROS Mock**: Simulates a modern cloud-based route optimization service
- **WMS Mock**: Simulates a proprietary WMS with TCP socket interface

### Infrastructure

- **PostgreSQL**: Database for persistent storage
- **RabbitMQ**: Message broker for asynchronous communication
- **Traefik**: API Gateway for routing and load balancing

## System Flow

1. Customer places an order via web frontend or mobile app
2. Order is processed by the Order Service
3. Order is published to RabbitMQ message queue
4. Adapter services consume the order message:
   - CMS Adapter creates the order in the CMS
   - ROS Adapter generates optimal delivery routes
   - WMS Adapter prepares shipment in the warehouse
5. Transaction Service ensures ACID properties across distributed systems
6. Tracking Service provides real-time updates to customers

## Architecture Diagram

```
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│   Web/Mobile  │       │     Order     │       │  Transaction  │
│    Frontend   │───────│    Service    │───────│    Service    │
└───────────────┘       └───────────────┘       └───────────────┘
                               │                        │
                               │                        │
                               ▼                        │
                        ┌───────────────┐              │
                        │    RabbitMQ   │              │
                        │ Message Broker│              │
                        └───────────────┘              │
                               │                        │
                               │                        │
         ┌───────────┬─────────┼─────────┬───────────┐ │
         │           │         │         │           │ │
         ▼           ▼         ▼         ▼           ▼ ▼
┌───────────────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────────────┐
│ CMS Adapter   │ │  ROS  │ │  WMS  │ │Tracking│ │   Database    │
│ (SOAP/XML)    │ │Adapter│ │Adapter│ │Service │ │  (PostgreSQL) │
└───────────────┘ └───────┘ └───────┘ └───────┘ └───────────────┘
         │           │         │         │
         │           │         │         │
         ▼           ▼         ▼         ▼
┌───────────────┐ ┌───────┐ ┌───────┐ ┌───────────────┐
│  CMS System   │ │  ROS  │ │  WMS  │ │   WebSocket   │
│  (Legacy)     │ │System │ │System │ │   Clients     │
└───────────────┘ └───────┘ └───────┘ └───────────────┘
```

## Team Workflow Guide

### 1️⃣ Clone the Project

Every team member must have the latest code:

```bash
git clone https://github.com/YOUR_USERNAME/swiftlogistics.git
cd swiftlogistics
git pull origin main
```

### 2️⃣ Run the System

Start all services:

```bash
docker-compose up -d
```

This starts all services including Traefik, PostgreSQL, RabbitMQ, adapters, and mock services.

Check if containers are running:

```bash
docker ps
```

### 3️⃣ Access Services

- **Order Service:** http://localhost/orders
- **CMS Adapter:** http://localhost/cms
- **ROS Adapter:** http://localhost/ros
- **WMS Adapter:** http://localhost/wms
- **Tracking Service:** http://localhost/tracking
- **Transaction Service:** http://localhost/transactions
- **RabbitMQ Management:** http://localhost:15672 (guest/guest)
- **Traefik Dashboard:** http://localhost:8080

### 4️⃣ API Documentation

Each service exposes a Swagger UI for API documentation:

- Order Service: http://localhost/orders/docs
- CMS Adapter: http://localhost/cms/docs
- ROS Adapter: http://localhost/ros/docs
- WMS Adapter: http://localhost/wms/docs
- Tracking Service: http://localhost/tracking/docs
- Transaction Service: http://localhost/transactions/docs

## Testing the System

To test the complete order flow:

1. Create an order via the Order Service
2. Check the order status in the CMS Adapter
3. View the generated route in the ROS Adapter
4. Track the shipment in the WMS Adapter
5. Monitor real-time updates via the Tracking Service WebSocket

## Security Considerations

- All internal services communicate over a private Docker network
- External access is controlled via Traefik API Gateway
- Authentication and authorization to be implemented as needed

## Monitoring and Logging

- Docker logs can be accessed for each service
- Structured logging is implemented for better observability
- Health check endpoints are available for all services
