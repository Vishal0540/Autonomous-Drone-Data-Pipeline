from enum import Enum


class OperationalStatus(int, Enum):
    IDLE = 0
    IN_DELIVERY = 1
    CHARGING = 2
    MAINTENANCE = 3
    HARDWARE_ALERT = 4

class HardwareError(int, Enum):
    MOTOR_FAILURE = 0
    BATTERY_MALFUNCTION = 1
    GPS_ERROR = 2
    COMMUNICATION_LOSS = 3
    SENSOR_FAILURE = 4