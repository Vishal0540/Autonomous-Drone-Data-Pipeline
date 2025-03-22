import sys
import os
import random
import math
import time
import uuid
import argparse
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime
import signal

from pydantic import BaseModel
from sqlalchemy import create_engine

# Add this import for better error handling
from kafka.errors import KafkaError, KafkaTimeoutError

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_models.drone_deliveries import DroneDelivery
from config import db_client, DRONE_TELEMETRY_TOPIC, KAFKA_PRODUCER as producer
from enums.drone_enums import OperationalStatus, HardwareError
from pydantic_models.drone_models import DeliveryOrder, Coordinates, DroneTelemetry

topic_name = DRONE_TELEMETRY_TOPIC


# Update base stations for charging and maintenance locations
BANGALORE_BASE_STATIONS = [
    {"name": "North Bangalore Base", "latitude": 13.05, "longitude": 77.65},
    {"name": "South Bangalore Base", "latitude": 12.85, "longitude": 77.55},
    {"name": "East Bangalore Base", "latitude": 12.95, "longitude": 77.75},
    {"name": "West Bangalore Base", "latitude": 12.95, "longitude": 77.55},
    {"name": "Central Bangalore Base", "latitude": 12.97, "longitude": 77.60}
]

def get_nearest_base_station(lat, lon):
    """Find the nearest base station for the drone"""
    min_distance = float('inf')
    nearest_base = None
    
    for base in BANGALORE_BASE_STATIONS:
        base_lat, base_lon = base["latitude"], base["longitude"]
        # Simple Euclidean distance as an approximation
        distance = math.sqrt((base_lat - lat)**2 + (base_lon - lon)**2)
        if distance < min_distance:
            min_distance = distance
            nearest_base = base
    
    return nearest_base

def datetime_from_timestamp(timestamp):
    """Convert timestamp to datetime object for DB storage"""
    return datetime.fromtimestamp(timestamp)

def get_utc_timestamp_int():
    """Get current UTC timestamp as integer"""
    return int(time.time())

def random_bangalore_location():
    """Generate random coordinates within Bangalore area"""
    # Approximate Bangalore boundaries
    min_lat, max_lat = 12.85, 13.05
    min_lon, max_lon = 77.50, 77.75
    
    lat = random.uniform(min_lat, max_lat)
    lon = random.uniform(min_lon, max_lon)
    return lat, lon

def destination_point(lat, lon, distance_km, bearing):
    """Calculate destination point given distance and bearing from start point"""
    # Earth radius in km
    R = 6371.0
    
    # Convert to radians
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)
    bearing_rad = math.radians(bearing)
    
    # Calculate new position
    lat2 = math.asin(math.sin(lat1) * math.cos(distance_km/R) +
                     math.cos(lat1) * math.sin(distance_km/R) * math.cos(bearing_rad))
    
    lon2 = lon1 + math.atan2(math.sin(bearing_rad) * math.sin(distance_km/R) * math.cos(lat1),
                             math.cos(distance_km/R) - math.sin(lat1) * math.sin(lat2))
    
    # Convert back to degrees
    lat2 = math.degrees(lat2)
    lon2 = math.degrees(lon2)
    
    return lat2, lon2

def create_delivery_order(drone_id):
    """
    Create a delivery order record using Pydantic models.
    """
    order_created_at = get_utc_timestamp_int()
    pickup_timestamp = get_utc_timestamp_int()
    delivery_timestamp = 0  # Not set until delivery completes
    
    # Generate pickup coordinate
    pickup_lat, pickup_lon = random_bangalore_location()
    # Generate a random trip distance (in km) and bearing to compute delivery coordinate
    trip_distance_km = random.uniform(1, 5)
    bearing = random.uniform(0, 360)
    delivery_lat, delivery_lon = destination_point(pickup_lat, pickup_lon, trip_distance_km, bearing)
    
    package_weight = round(random.uniform(1, 10), 2)
    
    delivery_order = DeliveryOrder(
        delivery_order_id=f"order_{uuid.uuid4()}",
        assigned_drone_id=drone_id,
        package_weight_kg=package_weight,
        pickup_latitude=pickup_lat,
        pickup_longitude=pickup_lon,
        delivery_latitude=delivery_lat,
        delivery_longitude=delivery_lon,
        pickup_timestamp_utc=pickup_timestamp,
        delivery_timestamp_utc=delivery_timestamp,
        delivery_status="IN_PROGRESS",
        order_created_at_utc=order_created_at,
        trip_distance_km=trip_distance_km
    )
    
    # Insert into database
    insert_delivery_to_db(delivery_order)
    
    return delivery_order

def insert_delivery_to_db(delivery_order):
    """Insert the delivery order into the database"""
    db_delivery = DroneDelivery(
        delivery_order_id=delivery_order.delivery_order_id,
        assigned_drone_id=delivery_order.assigned_drone_id,
        package_weight_kg=delivery_order.package_weight_kg,
        pickup_latitude=delivery_order.pickup_latitude,
        pickup_longitude=delivery_order.pickup_longitude,
        delivery_latitude=delivery_order.delivery_latitude,
        delivery_longitude=delivery_order.delivery_longitude,
        pickup_timestamp_utc=datetime_from_timestamp(delivery_order.pickup_timestamp_utc),
        delivery_timestamp_utc=None,  # Will be updated when delivery completes
        delivery_status=delivery_order.delivery_status,
        order_created_at_utc=datetime_from_timestamp(delivery_order.order_created_at_utc),
        order_by=None  # Optional field not used in simulation
    )
    
    with db_client.session_scope() as session:
        session.add(db_delivery)

def stream_drone_telemetry(delivery_order, drone_speed=10, sample_rate=1, max_alt=100, include_hardware_errors=False, dry_run=False):
    try:
        """
        Simulate and stream drone telemetry data using the delivery order.
        
        The telemetry data includes a linear flight path from the pickup
        coordinate to the delivery coordinate with horizontal and vertical speed components.
        
        If hardware errors are included, the drone will stop in the middle of the journey.
        If dry_run is True, data will only be printed, not sent to Kafka.
        """
        pickup_lat, pickup_lon = delivery_order.pickup_latitude, delivery_order.pickup_longitude
        delivery_lat, delivery_lon = delivery_order.delivery_latitude, delivery_order.delivery_longitude
        trip_distance_km = delivery_order.trip_distance_km
        
        # Base speed used for overall time calculation
        base_speed = drone_speed
        
        # Compute total flight time based on constant speed (convert km to m)
        flight_time = int((trip_distance_km * 1000) / base_speed)
        if flight_time == 0:
            flight_time = 1

        battery = 100.0  # initial battery percentage
        payload_weight = delivery_order.package_weight_kg
        operational_status = OperationalStatus.IN_DELIVERY
        ascend_time = flight_time * 0.1
        descend_time = flight_time * 0.1
        cruise_time = flight_time - ascend_time - descend_time
        drone_id = delivery_order.assigned_drone_id
        
        # Randomly decide if this flight will have a hardware error
        hardware_error = None
        if include_hardware_errors:
            hardware_error = random.choice(list(HardwareError))
        
        # Calculate middle point of flight
        middle_point = flight_time // 2
        
        # Store previous values to calculate speeds
        prev_alt = 0
        prev_lat = pickup_lat
        prev_lon = pickup_lon
        prev_time = 0
        
        # Stream telemetry data at each sample point
        for t in range(0, flight_time + 1, sample_rate):
            fraction = t / flight_time
            # Linear interpolation between pickup and delivery coordinates
            current_lat = pickup_lat + (delivery_lat - pickup_lat) * fraction
            current_lon = pickup_lon + (delivery_lon - pickup_lon) * fraction
            
            # Altitude: simulate ascent, cruise, and descent
            if t <= ascend_time and ascend_time > 0:
                alt = (max_alt / ascend_time) * t
                # During ascent, horizontal speed is lower, vertical speed is higher
                horizontal_speed = base_speed * (0.5 + 0.5 * (t / ascend_time))
            elif t >= flight_time - descend_time and descend_time > 0:
                alt = max_alt - (max_alt / descend_time) * (t - (flight_time - descend_time))
                # During descent, horizontal speed is lower, vertical speed is higher (negative)
                horizontal_speed = base_speed * (0.5 + 0.5 * (1 - (t - (flight_time - descend_time)) / descend_time))
            else:
                alt = max_alt
                # During cruise, horizontal speed fluctuates, vertical speed is near zero
                horizontal_speed = base_speed * random.uniform(0.9, 1.3)
            
            # Calculate vertical speed based on altitude change (m/s)
            if t > 0:
                time_delta = t - prev_time
                vertical_speed = (alt - prev_alt) / time_delta
            else:
                # First point - estimate from initial ascent rate
                vertical_speed = max_alt / ascend_time if ascend_time > 0 else 0
            
            # Add some realistic variations to vertical speed
            vertical_speed += random.uniform(-0.5, 0.5)
            
            # Simulate battery drain linearly over the flight
            battery = max(0.0, battery - (20 / flight_time))
            
            # If hardware error is enabled and we've reached the middle point, stop the flight
            if include_hardware_errors and t >= middle_point:
                operational_status = OperationalStatus.HARDWARE_ALERT
                
                telemetry = DroneTelemetry(
                    drone_id=int(drone_id),
                    battery_percentage=round(battery, 2),
                    latitude=current_lat,
                    longitude=current_lon,
                    altitude=round(alt, 2),
                    operational_status=operational_status,
                    hardware_error=hardware_error,
                    payload_weight_kg=payload_weight,
                    timestamp_utc=get_utc_timestamp_int(),
                    horizontal_speed_mps=0,
                    vertical_speed_mps=0,
                    active_order_id=delivery_order.delivery_order_id
                )
                print(telemetry.model_dump_json())
                if not dry_run:
                    future = producer.send(topic_name, telemetry.model_dump_json())
                break
            
            operational_status = OperationalStatus.IN_DELIVERY
                
            if t == flight_time:
                operational_status = OperationalStatus.IDLE
                horizontal_speed = 0  # Landed, so speed is zero
                vertical_speed = 0

            telemetry = DroneTelemetry(
                drone_id=int(drone_id),
                battery_percentage=round(battery, 2),
                latitude=current_lat,
                longitude=current_lon,
                altitude=round(alt, 2),
                operational_status=operational_status,
                hardware_error=None,
                payload_weight_kg=payload_weight,
                timestamp_utc=get_utc_timestamp_int(),
                horizontal_speed_mps=round(horizontal_speed, 2),
                vertical_speed_mps=round(vertical_speed, 2),
                active_order_id=delivery_order.delivery_order_id if operational_status == OperationalStatus.IN_DELIVERY else ""
            )
            print(telemetry.model_dump_json())

            if not dry_run:
                print(topic_name)
                input("Press Enter to continue")
                future = producer.send(topic_name, telemetry.model_dump_json())
                time.sleep(sample_rate)
                
            # Store current values for next iteration
            prev_alt = alt
            prev_lat = current_lat
            prev_lon = current_lon
            prev_time = t
    except Exception as e:
        print(f"Error in stream_drone_telemetry: {e}")

def generate_charging_telemetry(drone_id, lat, lon, duration=60, sample_rate=5, dry_run=False):
    print(f"Generating charging telemetry for drone_{drone_id}")
    """Generate telemetry data for a drone that is charging"""
    base_station = get_nearest_base_station(lat, lon)
    battery = 20.0  # Starting with low battery
    print(base_station)
    print(duration)
    for t in range(0, duration, sample_rate):
        # Battery charges from 20% to 95% over the duration
        battery = 20.0 + (75.0 * t / duration)
        if battery >= 95.0:
            operational_status = OperationalStatus.IDLE
        else:
            operational_status = OperationalStatus.CHARGING
        print(t)
        telemetry = DroneTelemetry(
            drone_id=int(drone_id),
            battery_percentage=round(battery, 2),
            latitude=base_station["latitude"],
            longitude=base_station["longitude"],
            altitude=0.0,
            operational_status=operational_status,
            hardware_error=None,
            payload_weight_kg=0.0,
            timestamp_utc=get_utc_timestamp_int(),
            horizontal_speed_mps=0.0,
            vertical_speed_mps=0.0,
            active_order_id=""
        )
        
        print(telemetry.model_dump_json())
        if not dry_run:
            future = producer.send(topic_name, telemetry.model_dump())
            time.sleep(sample_rate)
                

def generate_maintenance_telemetry(drone_id, lat, lon, duration=120, sample_rate=10, dry_run=False):
    """Generate telemetry data for a drone that is in maintenance"""
    base_station = get_nearest_base_station(lat, lon)
    hardware_error = random.choice(list(HardwareError))
    
    for t in range(0, duration, sample_rate):
        # For the last sample, clear the error
        if t >= duration - sample_rate:
            operational_status = OperationalStatus.IDLE
            current_error = None
        else:
            operational_status = OperationalStatus.MAINTENANCE
            current_error = hardware_error
            
        telemetry = DroneTelemetry(
            drone_id=int(drone_id),
            battery_percentage=80.0,
            latitude=base_station["latitude"],
            longitude=base_station["longitude"],
            altitude=0.0,
            operational_status=operational_status,
            hardware_error=current_error,
            payload_weight_kg=0.0,
            timestamp_utc=get_utc_timestamp_int(),
            horizontal_speed_mps=0.0,
            vertical_speed_mps=0.0,
            active_order_id=""
        )
        
        print(telemetry.model_dump_json())
        if not dry_run:
            future = producer.send(topic_name, telemetry.model_dump())
            time.sleep(sample_rate)

# Add these global variables at the top level


def signal_handler(sig, frame):
    """Enhanced signal handler for graceful shutdown"""
    print("\nShutting down gracefully...")
    
    
    
    print("Shutdown complete.")
    sys.exit(0)

# Register the signal handler for SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    # Add command line argument parsing
    parser = argparse.ArgumentParser(description='Drone telemetry simulator')
    parser.add_argument('--dry-run', action='store_true', 
                        help='Run in dry run mode - print data but do not send to Kafka')
    parser.add_argument('--timeout', type=int, default=60,
                        help='Timeout in seconds for the simulation to run (default: 60)')
    args = parser.parse_args()
    
    if args.dry_run:
        print("Running in DRY RUN mode - data will not be sent to Kafka")

    db_client.create_table_if_not_exist(DroneDelivery)

    print(f"Dry run mode: {args.dry_run}")
    
    # Configuration parameters
    num_drones = 10  # Number of drones to simulate
    percent_normal = 70  # Percentage of drones on normal delivery
    percent_hardware_error = 10  # Percentage of drones with hardware errors
    percent_charging = 10  # Percentage of drones charging
    percent_maintenance = 10  # Percentage of drones in maintenance
    
    # Calculate counts based on percentages
    normal_count = int(num_drones * percent_normal / 100)
    error_count = int(num_drones * percent_hardware_error / 100)
    charging_count = int(num_drones * percent_charging / 100)
    maintenance_count = num_drones - normal_count - error_count - charging_count
    
    # Generate drone IDs (range from 1 to 1000)
    drone_ids = random.sample(range(1, num_drones+1), num_drones)
    
    normal_count = 1
    # Allocate drone IDs to different categories
    normal_drones = drone_ids[:normal_count]
    error_drones = drone_ids[normal_count:normal_count+error_count]
    charging_drones = drone_ids[normal_count+error_count:normal_count+error_count+charging_count]
    maintenance_drones = drone_ids[normal_count+error_count+charging_count:]

    # normal_drones = []
    error_drones = []
    charging_drones = []
    maintenance_drones = []

    print(f"Normal drones: {normal_drones}")
    print(f"Error drones: {error_drones}")
    print(f"Charging drones: {charging_drones}")
    print(f"Maintenance drones: {maintenance_drones}")

    # Handle normal deliveries and deliveries with hardware errors
    delivery_tasks = []
    for drone_id in normal_drones:
        order = create_delivery_order(drone_id)
        delivery_tasks.append((order, False))  # No hardware errors
        print(f"Created normal delivery for drone_{drone_id}")
    
    for drone_id in error_drones:
        order = create_delivery_order(drone_id)
        delivery_tasks.append((order, True))  # Include hardware errors
        print(f"Created delivery with potential error for drone_{drone_id}")

    # Make executor global so signal handler can access it
    # global executor, futures
    executor = ThreadPoolExecutor(max_workers=num_drones)
    futures = []
    try:
        # Start delivery simulations
        for order, include_errors in delivery_tasks:
            futures.append(executor.submit(stream_drone_telemetry, order, include_hardware_errors=include_errors, dry_run=args.dry_run))
        
        # Start charging simulations
        for drone_id in charging_drones:
            lat, lon = random_bangalore_location()
            futures.append(executor.submit(generate_charging_telemetry, drone_id, lat, lon, dry_run=args.dry_run))
            print(f"Started charging simulation for drone_{drone_id}")
        
        # Start maintenance simulations
        for drone_id in maintenance_drones:
            lat, lon = random_bangalore_location()
            futures.append(executor.submit(generate_maintenance_telemetry, drone_id, lat, lon, dry_run=args.dry_run))
            print(f"Started maintenance simulation for drone_{drone_id}")
        
        print("Simulation running. Press Ctrl+C to stop.")
    
        done, not_done = wait(futures)
        
        # Cancel any tasks that didn't complete within the timeout
        for future in not_done:
            future.cancel()
            
    except KeyboardInterrupt:
        print("\nInterrupted by user. Shutting down...")
        # The signal handler will handle the cleanup
    finally:
        # Cleanup if not already handled by signal handler
        if executor:
            executor.shutdown(wait=False)
        
        if not args.dry_run and producer is not None:
            try:
                producer.flush(timeout=5)
                producer.close(timeout=5)
            except Exception as e:
                print(f"Error closing Kafka producer: {e}")
        
        print("Simulation complete.")
        sys.exit(0)