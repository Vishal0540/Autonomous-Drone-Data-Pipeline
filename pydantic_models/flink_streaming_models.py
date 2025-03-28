from pydantic import BaseModel
from typing import List

class ActivityPoint(BaseModel):
    sequence: int
    latitude: float
    longitude: float
    altitude: float
    timestamp_utc: int
    battery_percentage: float
    horizontal_speed: float
    vertical_speed: float

class RecentActivity(BaseModel):
    drone_id: int
    recent_points: List[ActivityPoint]
    avg_vertical_speed: float
    avg_horizontal_speed: float
    last_updated: int
