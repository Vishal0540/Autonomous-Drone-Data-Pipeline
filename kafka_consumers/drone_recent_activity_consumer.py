import asyncio
import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import cassandra_client, DRONE_RECENT_ACTIVITY_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from cassandra_utils.cassandra_queries import DroneRecentActivityQueries
from aiokafka import AIOKafkaConsumer           
from pydantic_models.flink_streaming_models import RecentActivity
from typing import List
from kafka_consumers.base_consumer import AsyncBatchConsumer

class DroneActivityTracker(AsyncBatchConsumer):
    
    def __init__(self, cassandra_client, batch_size: int = 70, batch_interval: int = 5):
        self.cassandra_session = cassandra_client.get_session()
        self.drone_activity_queries = DroneRecentActivityQueries(self.cassandra_session)
        consumer = AIOKafkaConsumer(
            DRONE_RECENT_ACTIVITY_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='drone_recent_activity_consumer_group',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        super().__init__(consumer, batch_size=batch_size, batch_interval=batch_interval)

    async def setup_consumer(self):
        await self.kafka_consumer.start()
        print("Starting to process drone activity data in batch mode...")
        print(f"Drone activity topic: {DRONE_RECENT_ACTIVITY_TOPIC}")
        print(f"Batch size: {self.batch_size}, Batch interval: {self.batch_interval} seconds")

    async def process_batch(self, messages: List[dict]):
        """Process messages in batch"""
        activity_objects = [RecentActivity(**msg) for msg in messages]
        self.drone_activity_queries.batch_insert(activity_objects)


async def main():
    tracker = DroneActivityTracker(cassandra_client)
    await tracker.consume()


if __name__ == "__main__":
    asyncio.run(main())
