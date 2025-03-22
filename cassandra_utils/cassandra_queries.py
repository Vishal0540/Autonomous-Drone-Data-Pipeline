from abc import ABC, abstractmethod
from cassandra.cluster import Cluster

class BaseCassandraQueries(ABC):
    CREATE_TABLE_QUERY = None

    """Abstract base class for async Cassandra queries"""
    def __init__(self, session):
        self.session = session
        # Note: We don't call create_table here directly in async version

    async def create_table(self):
        """Create table if it doesn't exist"""
        await self.session.execute_async(self.CREATE_TABLE_QUERY)
    
    @abstractmethod
    async def insert_data(self, data):
        """Abstract method to insert data"""
        pass


class DroneStatusQueries(AsyncBaseCassandraQueries):
    """Class containing async queries for drone telemetry table"""
    
    CREATE_TABLE_QUERY = """
        CREATE TABLE IF NOT EXISTS drone_status (
            drone_id int,
            battery_percentage double,
            latitude double,
            longitude double,
            altitude double,
            operational_status text,
            hardware_error text,
            payload_weight_kg double,
            timestamp_utc bigint,
            horizontal_speed_mps double,
            vertical_speed_mps double,
            active_order_id text,
            PRIMARY KEY (drone_id)
        )
    """
    
    INSERT_QUERY = """
        INSERT INTO drone_status         
        (drone_id, battery_percentage, latitude, longitude, altitude, 
        operational_status, hardware_error, payload_weight_kg, timestamp_utc,
        horizontal_speed_mps, vertical_speed_mps, active_order_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    async def insert_data(self, drone_status):
        """Insert drone status data asynchronously"""
        await self.session.execute_async(self.INSERT_QUERY, (
            drone_status.drone_id,
            drone_status.battery_percentage,
            drone_status.latitude,
            drone_status.longitude,
            drone_status.altitude,
            drone_status.operational_status.value,
            drone_status.hardware_error.value if drone_status.hardware_error else None,
            drone_status.payload_weight_kg,
            drone_status.timestamp_utc,
            drone_status.horizontal_speed_mps,
            drone_status.vertical_speed_mps,
            drone_status.active_order_id
        ))


class DroneRecentActivityQueries(AsyncBaseCassandraQueries):
    """Class containing async queries for drone recent activity table"""
    
    CREATE_TABLE_QUERY = """
        CREATE TABLE IF NOT EXISTS drone_recent_activity (
            drone_id int,
            recent_points list<frozen<map<text, double>>>,
            avg_vertical_speed double,
            avg_horizontal_speed double,
            last_updated bigint,
            PRIMARY KEY (drone_id)
        )
    """
    
    INSERT_QUERY = """
        INSERT INTO drone_recent_activity 
        (drone_id, recent_points, avg_vertical_speed, avg_horizontal_speed, last_updated)
        VALUES (%s, %s, %s, %s, %s)
    """

    async def insert_data(self, activity_data):
        """Insert drone activity data asynchronously"""
        points_list = []
        
        for point in activity_data.recent_points:
            points_list.append({
                'sequence': float(point.sequence),
                'latitude': point.latitude,
                'longitude': point.longitude,
                'altitude': point.altitude,
                'battery_percentage': point.battery_percentage,
                'horizontal_speed': point.horizontal_speed,
                'vertical_speed': point.vertical_speed,
                'timestamp': float(point.timestamp)
            })
            
        await self.session.execute_async(self.INSERT_QUERY, (
            activity_data.drone_id,
            points_list,
            activity_data.avg_vertical_speed,
            activity_data.avg_horizontal_speed,
            activity_data.last_updated
        ))