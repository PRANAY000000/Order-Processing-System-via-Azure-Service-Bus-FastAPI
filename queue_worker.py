import json
import os
from datetime import datetime
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# Connection strings and queue/topic names
QUEUE_CONNECTION_STR = "QUEUE_CONNECTION_STRING_HERE"
QUEUE_NAME = "order-queue"
TOPIC_CONNECTION_STR = "TOPIC_CONNECTION_STRING_HERE"
TOPIC_NAME = "order_distributed"

# Create logs directory
os.makedirs("logs", exist_ok=True)
log_path = os.path.join("logs", "queue_worker_log.txt")


def log_to_file(message: str):
    """Append logs to file with timestamp (UTF-8 encoding)."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(log_entry)


def process_queue():
    print(f"Listening to queue: {QUEUE_NAME}...")

    with ServiceBusClient.from_connection_string(QUEUE_CONNECTION_STR) as client:
        receiver = client.get_queue_receiver(queue_name=QUEUE_NAME)
        with receiver:
            for msg in receiver:
                try:
                    # Decode and parse JSON message
                    order_data = json.loads(str(msg))
                    print(f"Queue received order: {order_data}")

                    # Log to file
                    log_to_file(f"Order received: {order_data}")

                    # Send to topic for distribution
                    with ServiceBusClient.from_connection_string(TOPIC_CONNECTION_STR) as topic_client:
                        sender = topic_client.get_topic_sender(topic_name=TOPIC_NAME)
                        with sender:
                            topic_message = ServiceBusMessage(json.dumps(order_data))
                            sender.send_messages(topic_message)
                            print("Forwarded order to topic successfully.")
                            log_to_file("Order forwarded to topic successfully.")

                    receiver.complete_message(msg)

                except Exception as e:
                    print(f"[ERROR] Failed to process message: {e}")
                    log_to_file(f"[ERROR] Failed to process message: {e}")


if __name__ == "__main__":
    process_queue()
