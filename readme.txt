Project: Order Distributed System using Azure Service Bus

Process Overview
1. app.py – Receives new orders via FastAPI and sends them to the Azure Queue (order-queue).
2. queue_worker.py – Listens to the queue, processes each order, logs it, and forwards it to the Topic (order_distributed).
3. topic_worker.py – Listens to topic subscriptions (accounts, logistics, customer) and logs each received order separately.

Logs Generated
All logs are stored in the logs/ folder.


How to Run:

Install dependencies:
pip install -r requirements.txt

Ensure your connection strings are correctly added in:
QUEUE_CONNECTION_STR
TOPIC_CONNECTION_STR
CONNECTION_STR

Start FastAPI
fastapi dev app.py

POST http://127.0.0.1:8000/order

Example:
Content-Type: application/json
{
  "order_id": "ORD1001",
  "customer_email": "pranay@example.com",
  "items": ["Laptop", "Mouse"],
  "total": 49999.50,
  "status": "PENDING"
}


Run the Queue Worker: python queue_worker.py
→ It listens to the queue, processes new orders, and forwards them to the topic.

Run the Topic Worker: python topic_worker.py
→ It listens to each subscription (accounts, logistics, customer) and logs messages for each.

All data stored in Logs file


Outcome:
- Demonstrates Queue (point-to-point) and Topic (publish-subscribe) communication.
- Logs clearly show the flow of an order from request → processing → department-level distribution.
- Provides a realistic example of how distributed systems handle asynchronous messaging.