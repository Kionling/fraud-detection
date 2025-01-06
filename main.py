from kafka import KafkaProducer
import json
import time
import random 
from datetime import datetime


def generate_transaction():
    return {
        "transaction_id": f"txn_{random.randint(1000, 9999)}",
        "user_id": f"user_{random.randint(1, 10)}",
        "amount": round(random.uniform(10.0, 2000.0), 2),
        "timestamp": datetime.utcnow().isoformat(),
        "location": random.choice(["New York", "San Francisco", "London", "Berlin", "Tokyo"])
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Update with your Kafka server address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data as JSON
)

try:
    print("Starting transaction producer...")
    while True:
        transaction = generate_transaction()
        producer.send('transactions', transaction)  # Send transaction to the topic
        print(f"Produced: {transaction}")
        time.sleep(1)  # Simulate a 1-second delay between transactions
except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    producer.close()