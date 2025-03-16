import random
import string
import datetime
from typing import List
import uuid
import sys
import os


from sqlalchemy.exc import SQLAlchemyError



# Add the parent directory to sys.path to make config accessible
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import db_client
from db_models.drone_profile import DroneProfile

# Predefined lists of values for drone profiles
DRONE_MODELS = [
    "Phantom 4 Pro", "Mavic Air 2", "Inspire 2", "Mini 3 Pro", "Matrice 300 RTK",
    "Skydio 2", "Autel EVO II", "Parrot Anafi", "Yuneec Typhoon H", "PowerEgg X",
    "Blade Chroma", "Hubsan Zino", "Potensic D88", "Holy Stone HS720", "FIMI X8 SE"
]

COLORS = ["Black", "White", "Grey", "Red", "Blue", "Green", "Yellow", "Orange", "Silver", "Gold"]

MANUFACTURERS = [
    "DJI", "Skydio", "Autel Robotics", "Parrot", "Yuneec", "PowerVision", 
    "Blade", "Hubsan", "Potensic", "Holy Stone", "FIMI", "3DR", "GoPro", "Sony", "Ryze"
]

MAINTENANCE_STATUSES = ["OPERATIONAL", "NEEDS_SERVICE", "IN_REPAIR"]

def generate_hardware_id() -> str:
    """Generate a unique hardware ID for a drone using UUID."""
    prefix = random.choice(["DR", "UA", "DX", "HX", "QC"])
    uuid_str = str(uuid.uuid4())
    return f"{prefix}-{uuid_str}"

def generate_firmware_version() -> str:
    """Generate a random firmware version."""
    major = random.randint(1, 5)
    minor = random.randint(0, 9)
    patch = random.randint(0, 99)
    return f"v{major}.{minor}.{patch}"

def random_date(start_date, end_date) -> datetime.date:
    """Generate a random date between start_date and end_date."""
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_days = random.randrange(days_between_dates)
    return start_date + datetime.timedelta(days=random_days)

def generate_drone_profile() -> DroneProfile:
    """Generate a random drone profile."""
    now = datetime.datetime.now().date()
    five_years_ago = now - datetime.timedelta(days=5*365)
    
    manufacturing_date = random_date(five_years_ago, now)
    last_maintenance_date = random_date(manufacturing_date, now)

    
    return DroneProfile(
        model=random.choice(DRONE_MODELS),
        color=random.choice(COLORS),
        hardware_id=generate_hardware_id(),
        firmware_version=generate_firmware_version(),
        manufacturer=random.choice(MANUFACTURERS),
        manufacturing_date=manufacturing_date,
        max_payload_kg=round(random.uniform(0.2, 10.0), 2),
        battery_capacity_mah=random.randint(2000, 10000),
        camera_equipped=random.random() > 0.1,  # 90% of drones have cameras
        maintenance_status="OPERATIONAL",
        last_maintenance_date=last_maintenance_date,
        total_flight_hours=0
    )

def generate_drone_profiles(count: int = 5000) -> List[DroneProfile]:
    """Generate a specified number of drone profiles."""
    return [generate_drone_profile() for _ in range(count)]

def insert_drone_profiles():
    """Generate and insert drone profiles into the database."""
    # Ensure tables exist
    db_client.create_table_if_not_exist(DroneProfile)
    
    # Generate profiles
    print(f"Generating 5000 drone profiles...")
    profiles = generate_drone_profiles(5000)
    
    # Insert profiles into database
    print("Inserting profiles into database...")
    inserted_count = 0
    
    try:
        with db_client.session_scope() as session:
            # Add all profiles at once instead of one by one
            session.add_all(profiles)
            inserted_count = len(profiles)
            
            # Single commit for all profiles
            session.commit()
        
        print(f"Successfully inserted {inserted_count} drone profiles!")
    
    except SQLAlchemyError as e:
        print(f"Database error occurred: {str(e)}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    insert_drone_profiles()
    

