import asyncio
import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import cassandra_client, RED_ZONE_ALERT_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from cassandra_utils.cassandra_queries import RedZoneAlertQueries
from aiokafka import AIOKafkaConsumer   
from pydantic_models.flink_streaming_models import RedZoneAlert
from kafka_consumers.base_consumer import AsyncBaseConsumer


class RedZoneAlertConsumer(AsyncBaseConsumer):
    
    def __init__(self, cassandra_client):
        self.cassandra_session = cassandra_client.get_session()
        self.red_zone_alert_queries = RedZoneAlertQueries(self.cassandra_session)
        consumer = AIOKafkaConsumer(
            RED_ZONE_ALERT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='red_zone_alert_consumer_group',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        super().__init__(consumer)

    async def setup_consumer(self):
        await self.kafka_consumer.start()
        print("Starting to process red zone alerts...")
        print(f"Red zone alert topic: {RED_ZONE_ALERT_TOPIC}")
            
    async def process_message(self, message: dict):
        """Process individual red zone alert messages"""
        print(f"Received message: {message}")
        alert = RedZoneAlert(**message)
        self.red_zone_alert_queries.insert_data(alert)


async def main():
    consumer = RedZoneAlertConsumer(cassandra_client)
    await consumer.consume()


if __name__ == "__main__":
    asyncio.run(main())