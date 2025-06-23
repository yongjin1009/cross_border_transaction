from kafka import KafkaConsumer
import json


class KafkaConsumerClient:
    def __init__(self, topic_name: str, bootstrap_servers='localhost:9092', group_id='transaction_group'):
        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='latest',  # or 'latest'
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    def consume_messages(self):
        print("Listening for messages...")
        for message in self.consumer:
            print(f"Received: {message.value}")


def main():
    topic = 'transactions'
    consumer_client = KafkaConsumerClient(topic_name=topic)
    consumer_client.consume_messages()


if __name__ == '__main__':
    main()
