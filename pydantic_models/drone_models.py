from typing import Optional
from pydantic import BaseModel
from enums.drone_enums import OperationalStatus, HardwareError

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