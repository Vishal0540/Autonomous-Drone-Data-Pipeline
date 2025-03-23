import os
from dotenv import load_dotenv
from db_utils.postgres_db import PGClient
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


from cassandra_utils.cassandra_utils import CassandraClient
load_dotenv()


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "drone_db")
DB_USER = os.getenv("DB_USERNAME", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

db_client = PGClient(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    username=DB_USER,
    password=DB_PASSWORD,
    echo=False 
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

print(KAFKA_BOOTSTRAP_SERVERS)

# Try to create Kafka producer, set to None if no brokers available
KAFKA_PRODUCER = None
try:
    KAFKA_PRODUCER = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: x.encode('utf-8')
    )
except NoBrokersAvailable:
    print("Warning: No Kafka brokers available. KAFKA_PRODUCER set to None.")


DRONE_TELEMETRY_TOPIC = "drone_telemetry"
DRONE_RECENT_ACTIVITY_TOPIC = "drone_recent_activity"
DRONE_STATUS_TOPIC = "drone_status"


cassandra_client = CassandraClient(
    hosts=["localhost"],
    keyspace="aerodronefleet",
    port=9042
)
                        



