import os
import sys
from typing import List
from urllib.parse import quote
import base64

from pyflink.common import Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from shapely.geometry import Point, Polygon

sys.path.append('../')
from pydantic_models.drone_models import DroneTelemetry
from pydantic_models.flink_streaming_models import RedZoneAlert
from flink_ops.red_zone_alert_ops import MultiZoneDroneMonitor
from config import (
    DRONE_TELEMETRY_TOPIC,
    RED_ZONE_ALERT_TOPIC
)
from flink_jobs.flink_job_utils import parse_dronetelemetry_protobuf


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Configure Kafka connector JAR
jar_path = os.path.abspath("flink-sql-connector-kafka-3.1.0-1.18.jar")
jar_path = jar_path.replace('\\', '/')
encoded_path = f"file:///{quote(jar_path)}"
env.add_jars(encoded_path)

# Kafka configuration
kafka_properties = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'test-group'
}

# Define red zone coordinates
red_zones = [
    (0, [(77.6090988027125, 12.976302962908646),
         (77.60301423593609, 12.981072597118361),
         (77.5971861311668, 12.976302962908646),
         (77.60301423593609, 12.970667712764975),
         (77.6090988027125, 12.976302962908646)])
]

# Configure Kafka source
kafka_source = KafkaSource.builder() \
    .set_topics(DRONE_TELEMETRY_TOPIC) \
    .set_properties(kafka_properties) \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

# Configure Kafka sink for red zone alerts
kafka_red_zone_sink = KafkaSink.builder() \
    .set_bootstrap_servers(kafka_properties['bootstrap.servers']) \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()    
        .set_topic(RED_ZONE_ALERT_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    ) \
    .build()



# Create and process streams
source_stream = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Source"              
)


parsed_stream = source_stream.map(parse_dronetelemetry_protobuf)

# Process red zone monitoring
red_zone_stream = parsed_stream \
    .filter(lambda x: x.active_order_id != '') \
    .key_by(lambda x: (x.drone_id, x.active_order_id)) \
    .process(MultiZoneDroneMonitor(red_zones), output_type=Types.STRING())

# Connect stream to sink
red_zone_stream.sink_to(kafka_red_zone_sink)

print("Starting to process drone telemetry for red zone monitoring...")
env.execute("Red Zone Alert Processor")
