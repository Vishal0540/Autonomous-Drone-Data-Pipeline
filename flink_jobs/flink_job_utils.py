import base64
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pydantic_models.drone_models import DroneTelemetry
from protobuf_schemas import drone_telemetry_pb2


def parse_dronetelemetry_protobuf(raw_bytes):
    try:
        raw_bytes = base64.b64decode(raw_bytes)
        drone_telemetry_proto = drone_telemetry_pb2.DroneTelemetryProto()
        drone_telemetry_proto.ParseFromString(raw_bytes)
        
        drone_telemetry = DroneTelemetry(
            drone_id=drone_telemetry_proto.drone_id,
            battery_percentage=drone_telemetry_proto.battery_percentage,
            latitude=drone_telemetry_proto.latitude,
            longitude=drone_telemetry_proto.longitude,
            altitude=drone_telemetry_proto.altitude,
            operational_status=drone_telemetry_proto.operational_status,
            hardware_error=drone_telemetry_proto.hardware_error,
            payload_weight_kg=drone_telemetry_proto.payload_weight_kg,
            timestamp_utc=drone_telemetry_proto.timestamp_utc,
            horizontal_speed_mps=drone_telemetry_proto.horizontal_speed_mps,
            vertical_speed_mps=drone_telemetry_proto.vertical_speed_mps,
            active_order_id=drone_telemetry_proto.active_order_id,
        )
        return drone_telemetry
    except Exception as e:
        print(f"Protobuf parsing error: {e}")
        return "{}"