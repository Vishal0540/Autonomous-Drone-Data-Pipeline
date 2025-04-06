from abc import ABC, abstractmethod
from aiokafka import AIOKafkaConsumer
import asyncio
import time
from typing import List, Any

class AsyncBaseConsumer(ABC):
    """Abstract base class for async consumers"""
    
    def __init__(self, kafka_consumer: AIOKafkaConsumer):
        self.kafka_consumer = kafka_consumer

    @abstractmethod
    async def setup_consumer(self):
        """Abstract method for setting up the consumer"""
        pass

    @abstractmethod
    async def process_message(self, message: Any):
        """Abstract method for processing individual messages"""
        pass

    async def consume(self):
        """Common consume method for immediate processing"""
        await self.setup_consumer()
        try:
            async for message in self.kafka_consumer:
                try:
                    print(f"Received message: {message.value}")
                    await self.process_message(message.value)
                except Exception as e:
                    print(f"Error processing message: {e}")
        finally:
            await self.kafka_consumer.stop()


class AsyncBatchConsumer(ABC):
    """Abstract base class for batch processing consumers"""
    
    def __init__(self, kafka_consumer: AIOKafkaConsumer, batch_size: int = 100, batch_interval: int = 5):
        self.kafka_consumer = kafka_consumer
        self.queue = asyncio.Queue()
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self.processing = False

    @abstractmethod
    async def setup_consumer(self):
        """Abstract method for setting up the consumer"""
        pass

    @abstractmethod
    async def process_batch(self, batch: List[Any]):
        """Abstract method for processing batches of messages"""
        pass

    async def batch_processor(self):
        """Batch processing logic"""
        self.processing = True
        while self.processing:
            batch: List[dict] = []
            try:
                start_time = time.time()
                while (time.time() - start_time < self.batch_interval and 
                       len(batch) < self.batch_size):
                    try:
                        message = await asyncio.wait_for(self.queue.get(), timeout=0.1)
                        batch.append(message)
                        self.queue.task_done()
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        print(f"Error getting message from queue: {e}")
                
                if batch:
                    print(f"Processing batch of {len(batch)} messages")
                    try:
                        await self.process_batch(batch)
                        print(f"Successfully processed batch of {len(batch)} messages")
                    except Exception as e:
                        print(f"Error processing batch: {e}")

            except Exception as e:
                print(f"Error in batch processing: {e}")

    async def consume(self):
        """Common consume method for batch processing"""
        await self.setup_consumer()
        processor_task = asyncio.create_task(self.batch_processor())
        
        try:
            async for message in self.kafka_consumer:
                try:
                    print(f"Received message: {message.value}")
                    await self.queue.put(message.value)
                except Exception as e:
                    print(f"Error queueing message: {e}")
        finally:
            self.processing = False
            await processor_task
            await self.kafka_consumer.stop()