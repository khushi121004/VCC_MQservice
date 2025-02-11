import pika
import json

# RabbitMQ Server Configuration
RABBITMQ_HOST = "192.168.122.46"
QUEUE_NAME = "orders"

def connect_to_rabbitmq():
    """Establish a connection to RabbitMQ."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)  # Durable queue for reliability
        return connection, channel
    except Exception as e:
        print(f" [!] Error connecting to RabbitMQ: {e}")
        return None, None

def send_order(order_data):
    """Send multiple order messages to RabbitMQ."""
    connection, channel = connect_to_rabbitmq()
    if not connection or not channel:
        return

    for order in order_data:
        try:
            message = json.dumps(order)
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2  # Makes the message persistent
                )
            )
            print(f" [✔] Sent Order: {order}")
        except Exception as e:
            print(f" [!] Failed to send order {order}: {e}")

    connection.close()

# Sample orders
orders = [
    {"order_id": 101, "item": "Laptop", "price": 75000},
    {"order_id": 102, "item": "Phone", "price": 50000},
    {"order_id": 103, "item": "Tablet", "price": 30000},
]

# Send orders to RabbitMQ
send_order(orders)
