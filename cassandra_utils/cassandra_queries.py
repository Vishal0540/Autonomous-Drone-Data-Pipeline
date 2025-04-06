from abc import ABC, abstractmethod
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from typing import List

class BaseCassandraQueries(ABC):
    CREATE_TABLE_QUERY = None

    """Abstract base class for Cassandra queries"""
    def __init__(self, session):
        self.session = session
        # Create table by default when initializing
        self.create_table()

    def create_table(self):
        """Create table if it doesn't exist"""
        self.session.execute(self.CREATE_TABLE_QUERY)
    
    @abstractmethod
    def insert_data(self, data):
        """Abstract method to insert data"""
        pass


class DroneStatusQueries(BaseCassandraQueries):
    
    CREATE_TABLE_QUERY = """
        CREATE TABLE IF NOT EXISTS drone_status (
            drone_id int,
            battery_percentage float,
            latitude float,
            longitude float,
            altitude float,
            operational_status int,
            hardware_error int,
            payload_weight_kg float,
            timestamp_utc bigint,
            horizontal_speed_mps float,
            vertical_speed_mps float,
            active_order_id text,
            PRIMARY KEY (drone_id)
        )
    """
    
    INSERT_QUERY = """
        INSERT INTO drone_status         
        (drone_id, battery_percentage, latitude, longitude, altitude, 
        operational_status, hardware_error, payload_weight_kg, timestamp_utc,
        horizontal_speed_mps, vertical_speed_mps, active_order_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    SELECT_ALL_QUERY = """
        SELECT * FROM drone_status
    """

    SELECT_BY_ID_QUERY = """
        SELECT * FROM drone_status WHERE drone_id = ?
    """
    
    def __init__(self, session):
        super().__init__(session)
        self.prepared_insert = session.prepare(self.INSERT_QUERY)
        self.prepared_select_by_id = session.prepare(self.SELECT_BY_ID_QUERY)
    
    def insert_data(self, drone_status):
        self.session.execute(self.prepared_insert, (
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

    def batch_insert(self, drone_status_list: List):
        batch = BatchStatement()
        
        for drone_status in drone_status_list:
            batch.add(self.prepared_insert, (
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
        
        self.session.execute(batch)

    def get_all_drones(self):
        return self.session.execute(self.SELECT_ALL_QUERY)

    def get_drone_by_id(self, drone_id: int):
        return self.session.execute(self.prepared_select_by_id, (drone_id,))


class DroneRecentActivityQueries(BaseCassandraQueries):
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
        VALUES (?, ?, ?, ?, ?)
    """

    def _prepare_point_list(self, activity_data):
        """Helper method to prepare points list for insertion"""
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
                'timestamp_utc': float(point.timestamp_utc)
            })
        return points_list

    def insert_data(self, activity_data):
        points_list = self._prepare_point_list(activity_data)
        self.session.execute(self.INSERT_QUERY, (
            activity_data.drone_id,
            points_list,
            activity_data.avg_vertical_speed,
            activity_data.avg_horizontal_speed,
            activity_data.last_updated
        ))

    def batch_insert(self, activity_data_list: List):
        batch = BatchStatement()
        prepared_stmt = self.session.prepare(self.INSERT_QUERY)

        for activity_data in activity_data_list:
            points_list = self._prepare_point_list(activity_data)
            batch.add(prepared_stmt, (
                activity_data.drone_id,
                points_list,
                activity_data.avg_vertical_speed,
                activity_data.avg_horizontal_speed,
                activity_data.last_updated
            ))

        self.session.execute(batch)


class RedZoneAlertQueries(BaseCassandraQueries):
    
    CREATE_TABLE_QUERY = """
        CREATE TABLE IF NOT EXISTS red_zone_alerts (
            alert_id text,
            drone_id int,
            delivery_order_id text,
            zone_id int,
            current_distance float,
            direction_confidence float,
            timestamp bigint,
            is_active boolean,
            PRIMARY KEY (alert_id)
        )
    """
    
    INSERT_QUERY = """
        INSERT INTO red_zone_alerts
        (alert_id, drone_id, delivery_order_id, zone_id, current_distance, 
        direction_confidence, timestamp, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    SELECT_ACTIVE_ALERTS = """
        SELECT * FROM red_zone_alerts WHERE is_active = true ALLOW FILTERING
    """

    SELECT_BY_DRONE_ID = """
        SELECT * FROM red_zone_alerts WHERE drone_id = ? ALLOW FILTERING
    """

    def __init__(self, session):
        super().__init__(session)
        self.prepared_insert = session.prepare(self.INSERT_QUERY)
        self.prepared_select_by_drone = session.prepare(self.SELECT_BY_DRONE_ID)

    def insert_data(self, alert_data):
        alert_id = f"{alert_data.drone_id}_{alert_data.delivery_order_id}"
        self.session.execute(self.prepared_insert, (
            alert_id,
            alert_data.drone_id,
            alert_data.delivery_order_id,
            alert_data.zone_id,
            alert_data.current_distance,
            alert_data.direction_confidence,
            alert_data.timestamp,                                                                   
            alert_data.is_active
        ))

    def batch_insert(self, alert_data_list: List):
        batch = BatchStatement()
        
        for alert_data in alert_data_list:
            alert_id = f"{alert_data.drone_id}_{alert_data.delivery_order_id}"
            batch.add(self.prepared_insert, (
                alert_id,
                alert_data.drone_id,
                alert_data.delivery_order_id,
                alert_data.zone_id,
                alert_data.current_distance,
                alert_data.direction_confidence,
                alert_data.timestamp,                                                                   
                alert_data.is_active
            ))
        
        self.session.execute(batch)

    def get_active_alerts(self):
        return self.session.execute(self.SELECT_ACTIVE_ALERTS)

    def get_alerts_by_drone(self, drone_id: int):
        return self.session.execute(self.prepared_select_by_drone, (drone_id,))
