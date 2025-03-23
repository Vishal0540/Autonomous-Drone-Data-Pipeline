import osmnx as ox
from shapely.geometry import Polygon

from sqlalchemy import (
    Column,
    Integer,
    String,
    text
)
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry


Base = declarative_base()

class RedZone(Base):
    __tablename__ = "red_zones"

    zone_id = Column(Integer, primary_key=True, autoincrement=True)
    zone_name = Column(String(255))
    city = Column(String(255))
    state = Column(String(255))
    country = Column(String(255))
    zone_type = Column(Integer) 
    polygon = Column(Geometry("POLYGON", srid=4326))
