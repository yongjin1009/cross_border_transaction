import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

methods = ['CREDIT_CARD', 'PAYPAL', 'BANK_TRANSFER']
country_currency = {
    'US': 'USD',
    'CN': 'CNY',
    'MY': 'MYR',
    'SG': 'SGD'
}


class KafkaProducerClient:
    def __init__(self, topic_name: str, bootstrap_servers='localhost:9092'):
        self.topic_name = topic_name
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_message(self, message: dict):
        print(f"Producing: {message}")
        self.producer.send(self.topic_name, message)
        self.producer.flush()


def generate_transaction():
    is_cross = random.choice([False])
    origin = random.choice(list(country_currency.keys()))

    if is_cross:
        destination = random.choice([c for c in country_currency.keys() if c != origin])
    else:
        destination = origin

    if is_cross:
        # 3% chance of third-party currency (not from destination)
        if random.random() < 0.03:
            valid_currencies = [
                cur for cur in country_currency.values() if cur not in (country_currency[destination])
            ]
            currency = random.choice(valid_currencies)
        else:
            # Usually use destination currency
            currency = country_currency[destination]
    else:
        # Local: always use origin currency
        currency = country_currency[origin]
        
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": int(time.time()),
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "currency": currency,
        "sender_id": f"R{random.randint(1000, 9999)}",
        "country": origin,
        "payment_method": random.choice(methods),
        "receiver_id": f"R{random.randint(1000, 9999)}",
        "is_cross_border": is_cross,
        "destination_country": destination
    }


def main():
    topic = "transactions"
    kafka_client = KafkaProducerClient(topic_name=topic)

    while True:
        txn = generate_transaction()
        kafka_client.send_message(txn)
        time.sleep(1)

if __name__ == "__main__":
    main()
