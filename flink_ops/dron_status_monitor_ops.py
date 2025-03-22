import datetime
import statistics
import sys



from pyflink.datastream.functions import ProcessWindowFunction, ReduceFunction

sys.path.append('../')
from pydantic_models.drone_models import DroneTelemetry
from pydantic_models.flink_streaming_models import ActivityPoint, RecentActivity


class RecentDroneStatus(ReduceFunction):
    def reduce(self, value1: DroneTelemetry, value2: DroneTelemetry):
        return value2.model_dump_json()



class RecentActivityWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        recent_activities = list(elements)
        
        if len(recent_activities) > 180:
            recent_activities = recent_activities[-180:]
            
        # Calculate average vertical and horizontal speeds
        vertical_speeds = [telemetry.vertical_speed_mps for telemetry in recent_activities]
        horizontal_speeds = [telemetry.horizontal_speed_mps for telemetry in recent_activities]
        
        avg_vertical_speed = statistics.mean(vertical_speeds) if vertical_speeds else 0
        avg_horizontal_speed = statistics.mean(horizontal_speeds) if horizontal_speeds else 0
        
        activity_points = [
            ActivityPoint(
                sequence=idx,  
                latitude=telemetry.latitude,
                longitude=telemetry.longitude,
                altitude=telemetry.altitude,
                datetime=datetime.datetime.fromtimestamp(telemetry.timestamp_utc).strftime('%Y-%m-%d %H:%M:%S'),
                battery_percentage=telemetry.battery_percentage,
                horizontal_speed=telemetry.horizontal_speed_mps,
                vertical_speed=telemetry.vertical_speed_mps,
                timestamp=telemetry.timestamp_utc
            ) for idx, telemetry in enumerate(recent_activities)
        ]
            
        recent_activity = RecentActivity(
            drone_id=key,
            recent_points=activity_points,
            avg_vertical_speed=round(avg_vertical_speed, 2),
            avg_horizontal_speed=round(avg_horizontal_speed, 2)
        )

        print(recent_activity.model_dump())
        yield recent_activity.model_dump_json()

