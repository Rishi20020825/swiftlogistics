# SwiftLogistics Project

## Team Workflow Guide

### 1️⃣ Clone the Project

Every team member must have the latest code:

```bash
git clone https://github.com/YOUR_USERNAME/swiftlogistics.git
cd swiftlogistics
git pull origin main
```

---

### 2️⃣ Create Your Own Branch

Never work directly on `main`. Each person works on their own feature branch:

```bash
git checkout -b <your_name>-branch
```

**Example:**

```bash
git checkout -b person3-order-service
```

---

### 3️⃣ Run Infrastructure Locally

Each member must start their own copy of the base infrastructure:

```bash
docker-compose up -d
```

This starts Traefik, Postgres, RabbitMQ, and the placeholder frontend.

Check if containers are running:

```bash
docker ps
```

---

### 4️⃣ Access Services in Browser

- **Traefik dashboard:** [http://localhost:8080](http://localhost:8080)
- **RabbitMQ:** [http://localhost:15672](http://localhost:15672)  
  *(username: guest, password: guest)*
- **Frontend:** [http://localhost:3000](http://localhost:3000) *(if applicable)*

---

> **Note:** Always keep your branch up to date with `main` and follow the workflow for smooth
