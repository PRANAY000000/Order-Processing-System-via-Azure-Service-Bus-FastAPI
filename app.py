# app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import json
import uuid
import os

app = FastAPI(title="Order Processing System - Azure Service Bus")

# Azure Service Bus Configuration
SERVICE_BUS_CONNECTION_STR = "Endpoint=sb://pranayganagalla.servicebus.windows.net/;SharedAccessKeyName=queue_order;SharedAccessKey=zxA7OXQ+lAvw+j2F5ZKLcRvbjIkmbdBhW+ASbDV8tTE=;EntityPath=order-queue"
QUEUE_NAME = "order-queue"
TOPIC_NAME = "order_distributed"

# Create Service Bus client
servicebus_client = ServiceBusClient.from_connection_string(conn_str=SERVICE_BUS_CONNECTION_STR, logging_enable=True)

# Define request schema
class Order(BaseModel):
    order_id: str
    customer_email: EmailStr
    items: list
    total: float

@app.post("/place-order")
def place_order(order: Order):
    """
    Endpoint to send a new order into the queue.
    """
    try:
        message_data = {
            "order_id": order.order_id,
            "customer_email": order.customer_email,
            "items": order.items,
            "total": order.total,
            "status": "PENDING"
        }

        message = ServiceBusMessage(json.dumps(message_data))

        with servicebus_client:
            sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
            with sender:
                sender.send_messages(message)

        return {"status": "Order sent to queue successfully", "data": message_data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send order: {str(e)}")
