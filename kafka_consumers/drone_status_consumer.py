import asyncio
import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import cassandra_client, DRONE_STATUS_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from cassandra_utils.cassandra_queries import DroneStatusQueries
from aiokafka import AIOKafkaConsumer
from pydantic_models.drone_models import DroneTelemetry


class DroneStatusTracker:
    
    def __init__(self, cassandra_client):
        self.cassandra_session = cassandra_client.get_session()
        self.drone_status_queries = DroneStatusQueries(self.cassandra_session)
        self.consumer = None

    async def setup_consumer(self):
        self.consumer = AIOKafkaConsumer(
            DRONE_STATUS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='drone_status_consumer_group',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        await self.consumer.start()
        
    async def consume(self):
        await self.setup_consumer()
        print("Starting to process drone telemetry...")
        print(f"Drone telemetry topic: {DRONE_STATUS_TOPIC}")
        
        try:
            async for message in self.consumer:
                print(f"Received message: {message.value}")
                try:    
                    self.process_message(message.value)
                except Exception as e:
                    print(f"Error processing message: {e}")
        finally:
            await self.consumer.stop()
            
    def process_message(self, drone_data):
        self.drone_status_queries.insert_data(DroneTelemetry(**drone_data))


async def main():
    tracker = DroneStatusTracker(cassandra_client)
    await tracker.consume()


if __name__ == "__main__":
    asyncio.run(main())