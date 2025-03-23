import enum
import math
import random

import osmnx as ox
import shapely
from shapely.geometry import Polygon

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from enums.zones_enums import ZoneType
from db_models.zones_model import RedZone
from config import db_client

def get_city_center(city_name: str):
    """
    Use OSMnx to fetch the city's boundary and return the centroid as (lat, lon).
    """
    gdf = ox.geocode_to_gdf(city_name)
    if gdf.empty:
        raise ValueError(f"City '{city_name}' not found.")
    centroid = gdf.iloc[0].geometry.centroid
    return centroid.y, centroid.x  # (lat, lon)

def create_random_polygon(center_lat, center_lon, avg_radius_m, num_sides, variation=0.3):
    """
    Create a random polygon with `num_sides` around (center_lat, center_lon).
    - avg_radius_m: average radius in meters.
    - variation: fraction that randomizes each vertex's distance.
    Returns a Shapely Polygon in lat/lon (EPSG:4326).
    """
    deg_per_meter = 1.0 / 111320.0  # approximate
    angle_step = 2 * math.pi / num_sides
    points = []
    for i in range(num_sides):
        angle = i * angle_step
        radius_m = avg_radius_m * (1 + random.uniform(-variation, variation))
        radius_deg = radius_m * deg_per_meter
        vertex_lon = center_lon + radius_deg * math.cos(angle)
        vertex_lat = center_lat + radius_deg * math.sin(angle)
        points.append((vertex_lon, vertex_lat))
    return Polygon(points)

def generate_random_zones(city_name, num_zones=10):
    """
    Generate random zones for a given city. Returns a list of RedZone objects.
    """
    center_lat, center_lon = get_city_center(city_name)
    zones = []
    for i in range(num_zones):
        # Randomly pick a polygon type
        sides = random.choice([4, 5, 6])  # square, pentagon, hexagon
        zone_type = {
            4: ZoneType.SQUARE,
            5: ZoneType.PENTAGON,
            6: ZoneType.HEXAGON
        }[sides]

        # Random radius between 300 and 1000 meters
        avg_radius = random.uniform(300, 1000)
        poly = create_random_polygon(center_lat, center_lon, avg_radius, sides)

        # Convert to WKT with SRID=4326 for GeoAlchemy2
        wkt = f"SRID=4326;{poly.wkt}"

        zone_obj = RedZone(
            zone_name=f"Random Zone {i+1}",
            city=city_name,
            state="RandomState",
            country="RandomCountry",
            zone_type=zone_type.value,
            polygon=wkt
        )
        zones.append(zone_obj)
    return zones

if __name__ == "__main__":
    # Use the pg_client from config instead of creating a new connection
    engine = db_client.engine
    
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Generate random zones for a city
    city_name = "Bangalore, Karnataka, India"
    random_zones = generate_random_zones(city_name, num_zones=5)

    # Insert them into DB
    for rz in random_zones:
        session.add(rz)
    session.commit()
    print("Inserted random red zones into the database.")

    lat_query, lon_query = 37.77, -122.42  # near SF
    distance_m = 2000  # 2 km
    query_sql = text(f"""
        SELECT zone_id, zone_name, city, state, country, zone_type,
               ST_AsText(polygon) AS wkt
        FROM red_zones
        WHERE ST_DWithin(
            polygon::geography,
            ST_MakePoint(:lon, :lat)::geography,
            :dist
        )
    """)
    result = session.execute(query_sql, {"lon": lon_query, "lat": lat_query, "dist": distance_m})
    rows = result.fetchall()

    print(f"\nZones within {distance_m} m of ({lat_query}, {lon_query}):")
    for row in rows:
        print(dict(row))

    session.close()
