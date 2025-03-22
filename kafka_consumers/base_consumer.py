from abc import ABC, abstractmethod
from aiokafka import AIOKafkaConsumer

class AsyncBaseConsumer(ABC):
    """Abstract base class for async consumers"""
    
    def __init__(self, kafka_consumer: AIOKafkaConsumer):
        self.kafka_consumer = kafka_consumer
    
    @abstractmethod
    async def consume(self):
        """Abstract method for consuming messages"""
        pass
        
    @abstractmethod
    async def process_message(self, message):
        """Abstract method for processing messages"""
        pass
    