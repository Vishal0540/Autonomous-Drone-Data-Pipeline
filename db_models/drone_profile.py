from sqlalchemy import Column, String, Float, Date, Boolean, Enum, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DroneProfile(Base):
    __tablename__ = "drone_profiles"
    
    drone_id = Column(Integer, primary_key=True, autoincrement=True)
    model = Column(String(100), nullable=False)
    color = Column(String(50))
    hardware_id = Column(String(100), unique=True, nullable=False)
    firmware_version = Column(String(50))
    manufacturer = Column(String(100))
    manufacturing_date = Column(Date)
    max_payload_kg = Column(Float)
    battery_capacity_mah = Column(Integer)
    camera_equipped = Column(Boolean, default=False)
    maintenance_status = Column(Enum('OPERATIONAL', 'NEEDS_SERVICE', 'IN_REPAIR', name='maintenance_status_enum'))
    last_maintenance_date = Column(Date)
    total_flight_hours = Column(Float)