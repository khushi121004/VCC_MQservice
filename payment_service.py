import pika
import json

# RabbitMQ Server Configuration
RABBITMQ_HOST = "192.168.122.46"
QUEUE_NAME = "orders"

def connect_to_rabbitmq():
    """Establish a connection to RabbitMQ and declare a queue."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)  # Durable queue to persist orders
        return connection, channel
    except Exception as e:
        print(f" [!] Error connecting to RabbitMQ: {e}")
        return None, None

def process_payment(ch, method, properties, body):
    """Simulate payment processing."""
    try:
        order = json.loads(body)
        print(f" [✔] Processing Payment for Order: {order}")

        # Simulate successful processing
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge message after processing
        print(f" [✔] Payment Processed Successfully for Order ID: {order['order_id']}\n")

    except Exception as e:
        print(f" [!] Error processing order: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)  # Requeue the message if it fails

def start_consumer():
    """Start consuming messages from RabbitMQ."""
    connection, channel = connect_to_rabbitmq()
    if not connection or not channel:
        return

    channel.basic_qos(prefetch_count=1)  # Ensures only one order is processed at a time
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_payment, auto_ack=False)

    print(" [*] Waiting for orders. To exit press CTRL+C")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n [!] Stopping consumer...")
        channel.stop_consuming()
        connection.close()

# Start the consumer
start_consumer()
