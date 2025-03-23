import asyncio
import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import cassandra_client, DRONE_RECENT_ACTIVITY_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from cassandra_utils.cassandra_queries import DroneRecentActivityQueries
from aiokafka import AIOKafkaConsumer           
from pydantic_models.flink_streaming_models import RecentActivity

class DroneActivityTracker:
    
    def __init__(self, cassandra_client):
        self.cassandra_session = cassandra_client.get_session()
        self.drone_activity_queries = DroneRecentActivityQueries(self.cassandra_session)
        self.consumer = None

    async def setup_consumer(self):
        self.consumer = AIOKafkaConsumer(
            DRONE_RECENT_ACTIVITY_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='drone_recent_activity_consumer_group',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        await self.consumer.start()
        
    async def consume(self):
        await self.setup_consumer()
        print("Starting to process drone activity data...")
        print(f"Drone activity topic: {DRONE_RECENT_ACTIVITY_TOPIC}")
        
        try:
            async for message in self.consumer:
                print(f"Received activity: {message.value}")
                try:    
                    self.process_message(message.value)
                except Exception as e:
                    print(f"Error processing activity: {e}")
        finally:
            await self.consumer.stop()
            
    def process_message(self, activity_data):
        self.drone_activity_queries.insert_data(RecentActivity(**activity_data))


async def main():
    tracker = DroneActivityTracker(cassandra_client)
    await tracker.consume()


if __name__ == "__main__":
    asyncio.run(main())
