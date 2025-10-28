import json
import os
import threading
from datetime import datetime
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# ✅ Namespace-level connection string (not topic-specific)
CONNECTION_STR = "CONNECTION_STRING_HERE"
TOPIC_NAME = "order_distributed"
SUBSCRIPTIONS = ["accounts", "logistics", "customer"]

# ✅ Log setup
os.makedirs("logs", exist_ok=True)
log_path = os.path.join("logs", "topic_worker_log.txt")


def log_to_file(message: str):
    """Append logs to file with timestamp (UTF-8 encoding)."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(log_entry)


def process_subscription(subscription_name):
    print(f"Listening to subscription: {subscription_name}")

    with ServiceBusClient.from_connection_string(CONNECTION_STR) as client:
        receiver = client.get_subscription_receiver(
            topic_name=TOPIC_NAME,
            subscription_name=subscription_name
        )
        with receiver:
            for msg in receiver:
                try:
                    order_data = json.loads(str(msg))
                    print(f"Subscription '{subscription_name}' received: {order_data}")
                    log_to_file(f"Subscription '{subscription_name}' processed order: {order_data}")

                    # Optional confirmation log file (for audit/history)
                    with open(f"logs/{subscription_name}_history.txt", "a", encoding="utf-8") as f:
                        f.write(f"[{datetime.now()}] Order processed: {order_data}\n")

                    receiver.complete_message(msg)
                except Exception as e:
                    print(f"[ERROR] {subscription_name}: {e}")
                    log_to_file(f"[ERROR] {subscription_name}: {e}")


def main():
    print("Starting topic workers...")
    threads = []
    for sub in SUBSCRIPTIONS:
        thread = threading.Thread(target=process_subscription, args=(sub,))
        threads.append(thread)
        thread.start()

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
