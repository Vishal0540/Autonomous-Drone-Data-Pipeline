from sqlalchemy import Column, String, Float, DateTime, Integer, Enum, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

from db_models.drone_profile import DroneProfile

Base = declarative_base()

class DroneDelivery(Base):
    __tablename__ = 'drone_deliveries'
    
    delivery_order_id = Column(String(42), primary_key=True)  # Will be populated with random UUID4
    assigned_drone_id = Column(Integer, ForeignKey(DroneProfile.drone_id), nullable=False)
    package_weight_kg = Column(Float, nullable=False)
    pickup_latitude = Column(Float, nullable=False)
    pickup_longitude = Column(Float, nullable=False)
    delivery_latitude = Column(Float, nullable=False)
    delivery_longitude = Column(Float, nullable=False)
    pickup_timestamp_utc = Column(DateTime, nullable=False)
    delivery_timestamp_utc = Column(DateTime, nullable=True)
    delivery_status = Column(Enum('IN_PROGRESS', 'COMPLETED', 'FAILED', name='delivery_status_enum'), nullable=False)
    order_created_at_utc = Column(DateTime, nullable=False)
    order_by = Column(Integer, nullable=True)
    
    def __repr__(self):
        return f"<DroneDelivery(delivery_order_id='{self.delivery_order_id}', assigned_drone_id='{self.assigned_drone_id}')>"