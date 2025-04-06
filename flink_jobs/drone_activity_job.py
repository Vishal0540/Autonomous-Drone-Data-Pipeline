
import os
import sys
from urllib.parse import quote


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
from pyflink.datastream.window import SlidingProcessingTimeWindows, TumblingProcessingTimeWindows

# Local application imports
sys.path.append('../')
from pydantic_models.drone_models import DroneTelemetry
from flink_ops.dron_status_monitor_ops import RecentActivityWindowFunction, RecentDroneStatus
from config import DRONE_STATUS_TOPIC, DRONE_RECENT_ACTIVITY_TOPIC, DRONE_TELEMETRY_TOPIC
from flink_jobs.flink_job_utils import parse_dronetelemetry_protobuf

# Configure environment

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

# Configure Kafka source
kafka_source = KafkaSource.builder() \
    .set_topics(DRONE_TELEMETRY_TOPIC) \
    .set_properties(kafka_properties) \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

# Configure Kafka sinks
kafka_status_sink = KafkaSink.builder() \
    .set_bootstrap_servers(kafka_properties['bootstrap.servers']) \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic(DRONE_STATUS_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    ) \
    .build()

# # Create Kafka sink for drone trail (updates every 5 seconds)
kafka_trail_sink = KafkaSink.builder() \
    .set_bootstrap_servers(kafka_properties['bootstrap.servers']) \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic(DRONE_RECENT_ACTIVITY_TOPIC)
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

# -------------------------------------------------------------------
# Branch 1: Drone Status Update using a 2-second tumbling window.
# -------------------------------------------------------------------
status_stream = parsed_stream \
    .key_by(lambda x: x.drone_id) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(2))) \
    .reduce(RecentDroneStatus()) \
    .map(lambda x: x.model_dump_json(), output_type=Types.STRING())

# -------------------------------------------------------------------
# Branch 2: Drone Trail Update using a 3-second tumbling window.
# -------------------------------------------------------------------
trail_stream = parsed_stream \
    .key_by(lambda x: x.drone_id) \
    .window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(3))) \
    .process(RecentActivityWindowFunction(), output_type=Types.STRING())

# Connect streams to sinks
trail_stream.sink_to(kafka_trail_sink)
status_stream.sink_to(kafka_status_sink)

print("Starting to process drone telemetry with tumbling windows (no watermarks)...")
env.execute("Drone Telemetry Processor with Tumbling Windows")
