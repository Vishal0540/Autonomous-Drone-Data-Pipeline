from typing import Optional
from pydantic import BaseModel
from enums.drone_enums import OperationalStatus, HardwareError

from protobuf_schemas.drone_telemetry_pb2 import DroneTelemetryProto

# Define Pydantic models for all data structures
class DeliveryOrder(BaseModel):
    delivery_order_id: str
    assigned_drone_id: int
    package_weight_kg: float
    pickup_latitude: float
    pickup_longitude: float
    delivery_latitude: float
    delivery_longitude: float
    pickup_timestamp_utc: int
    delivery_timestamp_utc: int
    delivery_status: str
    order_created_at_utc: int
    trip_distance_km: float

class Coordinates(BaseModel):
    latitude: float
    longitude: float


class DroneTelemetry(BaseModel):
    drone_id: int
    battery_percentage: float
    latitude: float
    longitude: float
    altitude: float
    operational_status: OperationalStatus
    hardware_error: Optional[HardwareError] = None
    payload_weight_kg: float
    timestamp_utc: int
    horizontal_speed_mps: float
    vertical_speed_mps: float
    active_order_id: str = "" 

    def to_protobuf(self):
        """Convert this Pydantic model instance to a protobuf message"""
        proto = DroneTelemetryProto(
            drone_id=self.drone_id,
            battery_percentage=self.battery_percentage,
            latitude=self.latitude,
            longitude=self.longitude,
            altitude=self.altitude,
            operational_status=self.operational_status.value,
            payload_weight_kg=self.payload_weight_kg,
            timestamp_utc=self.timestamp_utc,
            horizontal_speed_mps=self.horizontal_speed_mps,
            vertical_speed_mps=self.vertical_speed_mps,
            active_order_id=self.active_order_id
        )
        
        # Handle optional hardware_error field
        if self.hardware_error:
            proto.hardware_error = self.hardware_error.value
        
        return proto

