import json
import math
import sys


from shapely.geometry import Point, Polygon
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ListStateDescriptor

sys.path.append('../')

from pydantic_models.flink_streaming_models import RedZoneAlert



class MultiZoneDroneMonitor(KeyedProcessFunction):
    def __init__(self, red_zones, proximity_threshold=4000.0, direction_threshold=0.85):
        print(f"Initializing MultiZoneDroneMonitor with {len(red_zones)} red zones")
        print(f"Proximity threshold: {proximity_threshold}m")
        print(f"Direction threshold: {direction_threshold}")
        self.red_zones = [(zone_id, Polygon(poly_coords)) for zone_id, poly_coords in red_zones]
        self.proximity_threshold = proximity_threshold
        self.direction_threshold = direction_threshold
        self.last_positions_state = None
        
    def open(self, runtime_context: RuntimeContext):
        print("Opening MultiZoneDroneMonitor and initializing state")

        # Define ListStateDescriptor using TUPLE
        positions_descriptor = ListStateDescriptor(
            "last_positions", 
            Types.STRING()
        )
        self.last_positions_state = runtime_context.get_list_state(positions_descriptor)

    def process_element(self, event, ctx):
        print(f"\nProcessing event for drone {event.drone_id}")
        current_position = (event.longitude, event.latitude)
        current_point = Point(current_position)
        current_time = event.timestamp_utc
        print(f"Current position: {current_position}, Time: {current_time}")
        
        # Manage position history
         # Manage position history
        positions = []
        for pos_str in self.last_positions_state.get():
            # Convert string back to list of floats
            pos = json.loads(pos_str)
            positions.append(pos)
        

        print("============================")
        print(positions)
        print("============================")
        print(f"Previous positions count: {len(positions)}")

        positions.append([current_position[0], current_position[1], current_time])

        
        if len(positions) > 5:
            positions = positions[-5:]
        print(f"Updated positions (last 5): {positions}")
        
        self.last_positions_state.clear()
        for pos in positions:
            self.last_positions_state.add(json.dumps(pos))
        
        
        # Check each red zone
        for zone_id, zone_polygon in self.red_zones:
            print(f"\nChecking red zone {zone_id}")
            # Calculate distance to this zone
            current_distance = self._distance_to_polygon(current_point, zone_polygon)
            print(f"Distance to zone: {current_distance}m")
            
            if current_distance <= self.proximity_threshold:
                print(f"Drone within proximity threshold ({self.proximity_threshold}m)")
                approaching_zone = False
                direction_confidence = 0.0
                
                # Only check direction if we have enough position history
                if len(positions) >= 3:
                    print("Calculating movement direction")
                    nearest_point = self._nearest_point_on_polygon(current_point, zone_polygon)
                    print(f"Nearest point on zone boundary: ({nearest_point.x}, {nearest_point.y})")
                    approaching_zone, direction_confidence = self._is_moving_toward_point(
                        positions, 
                        (nearest_point.x, nearest_point.y)
                    )
                    print(f"Approaching zone: {approaching_zone}, Confidence: {direction_confidence}")
                
                if approaching_zone and direction_confidence >= self.direction_threshold:
                    print("ALERT: Drone approaching red zone with high confidence!")
                    
                    red_zone_alert = RedZoneAlert(
                        drone_id=event.drone_id,
                        delivery_order_id=event.active_order_id,
                        zone_id=zone_id,
                        current_distance=current_distance,
                        direction_confidence=direction_confidence,
                        timestamp=current_time
                    ).model_dump_json()

                    print(red_zone_alert)
                    print("============================")               

                    yield red_zone_alert
    
    def _distance_to_polygon(self, point, polygon):
        if polygon.contains(point):
            print("Point is inside polygon")
            return 0
        
        # Get the nearest point on polygon boundary
        nearest_point = self._nearest_point_on_polygon(point, polygon)

        # print(f"Nearest point on polygon boundary: {nearest_point}")
        
        return self._haversine_distance(
            (point.y, point.x), 
            (nearest_point.y, nearest_point.x)
        )
    def _haversine_distance(self, point1, point2):
        """
        Points should be in (latitude, longitude) format.
        Returns distance in meters.
        """
        lat1, lon1 = point1
        lat2, lon2 = point2
        print(point1,point2)
        R = 6371000 
        
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c
    def _degrees_to_meters(self, degrees):
        return degrees * 111320
    
    def _nearest_point_on_polygon(self, point, polygon):
        return polygon.boundary.interpolate(polygon.boundary.project(point))
    
    def _is_moving_toward_point(self, positions, target_point):
        # print("\nCalculating movement direction")
        positions.sort(key=lambda x: x[2])
        
        p1 = (positions[-3][0], positions[-3][1])
        p2 = (positions[-1][0], positions[-1][1])
        
        movement_vector = (p2[0] - p1[0], p2[1] - p1[1])
        
        to_target = (
            target_point[0] - p2[0],
            target_point[1] - p2[1]
        )
        print(f"Vector to target: {to_target}")
        
        dot_product = movement_vector[0] * to_target[0] + movement_vector[1] * to_target[1]
        magnitude1 = math.sqrt(movement_vector[0]**2 + movement_vector[1]**2)
        magnitude2 = math.sqrt(to_target[0]**2 + to_target[1]**2)
        
        if magnitude1 == 0 or magnitude2 == 0:
            print("Zero magnitude detected, cannot determine direction")
            return False, 0.0
            
        cosine_similarity = dot_product / (magnitude1 * magnitude2)
        # print(f"Cosine similarity: {cosine_similarity}")
        
        return cosine_similarity > 0, abs(cosine_similarity)