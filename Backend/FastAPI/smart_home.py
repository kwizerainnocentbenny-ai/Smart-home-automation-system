"""
UNIFIED IoT CONTROL SYSTEM - COMPLETE BACKEND
══════════════════════════════════════════════════════════════
Combined Smart Home + Agriculture + Industrial + Smart City + Office Automation
Database: smart_home (single database for all systems)
Version: 5.0.0-UNIFIED

Run:      uvicorn main:app --reload --host 0.0.0.0 --port 8000
Docs:     http://localhost:8000/docs
══════════════════════════════════════════════════════════════

OPTIMIZATIONS:
  ✓ Connection pooling for database
  ✓ Response compression enabled
  ✓ Caching headers for static-like endpoints
  ✓ Optimized JSON serialization
  ✓ Reduced logging in production
  ✓ Faster database queries with indexes
  ✓ Async where possible
  ✓ Connection reuse
"""

from fastapi import FastAPI, HTTPException, Response, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
import mysql.connector
from mysql.connector import Error
from mysql.connector import pooling
from pydantic import BaseModel, validator, Field
from typing import List, Optional, Dict, Any, Union
import json
import logging
from datetime import datetime, timedelta
import re
import time
from functools import lru_cache
from collections import OrderedDict
import asyncio
import random

# Configure logging - less verbose in production
logging.basicConfig(
    level=logging.WARNING,  # Changed from INFO to WARNING for less noise
    format="[%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
#  DATABASE CONFIG WITH CONNECTION POOLING
# ══════════════════════════════════════════════════════════════
DB_CONFIG: Dict[str, Any] = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "smart_home",
    "pool_name": "mypool",
    "pool_size": 10,  # Connection pool for faster DB access
    "pool_reset_session": True,
    "connect_timeout": 5,  # Shorter timeout
    "use_pure": True,
}

# Create connection pool
try:
    connection_pool = mysql.connector.pooling.MySQLConnectionPool(**DB_CONFIG)
    log.info("✅ Database connection pool created")
except Error as e:
    log.error(f"❌ Failed to create connection pool: {e}")
    connection_pool = None


def get_db():
    """Get connection from pool - much faster than creating new connection"""
    if connection_pool:
        try:
            return connection_pool.get_connection()
        except Error as e:
            log.error(f"DB pool error: {e}")
            # Fallback to direct connection
            return mysql.connector.connect(**{k: v for k, v in DB_CONFIG.items() 
                                             if k not in ['pool_name', 'pool_size', 'pool_reset_session']})
    else:
        return mysql.connector.connect(**{k: v for k, v in DB_CONFIG.items() 
                                         if k not in ['pool_name', 'pool_size', 'pool_reset_session']})


# Cache for frequently accessed data
cache = OrderedDict()
CACHE_MAX_SIZE = 100
CACHE_TTL = 5  # seconds


def get_cached_or_set(key, func, *args, **kwargs):
    """Simple cache with TTL"""
    now = time.time()
    if key in cache:
        value, timestamp = cache[key]
        if now - timestamp < CACHE_TTL:
            return value
    
    value = func(*args, **kwargs)
    cache[key] = (value, now)
    
    # Keep cache size limited
    if len(cache) > CACHE_MAX_SIZE:
        cache.popitem(last=False)
    
    return value


# ══════════════════════════════════════════════════════════════
#  DATABASE SCHEMA - COMPLETE WITH ALL TABLES
# ══════════════════════════════════════════════════════════════

def init_database():
    """Initialize complete database schema for all systems"""
    conn = get_db()
    cur = conn.cursor()

    # ===== EXISTING SMART HOME TABLES =====
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rooms (
            id         INT AUTO_INCREMENT PRIMARY KEY,
            name       VARCHAR(100) UNIQUE NOT NULL,
            icon       VARCHAR(50)  DEFAULT 'mdi:door',
            room_type  VARCHAR(50)  DEFAULT 'home',  -- home, agriculture, industrial, city, office
            created_at TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_rooms_name (name),
            INDEX idx_rooms_type (room_type)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            id            INT AUTO_INCREMENT PRIMARY KEY,
            name          VARCHAR(100) NOT NULL,
            type          VARCHAR(50)  NOT NULL,
            room_id       INT          NOT NULL,
            current_state ENUM('ON','OFF') DEFAULT 'OFF',
            is_sensor     BOOLEAN      DEFAULT FALSE,
            sensor_value  VARCHAR(100) DEFAULT NULL,
            icon          VARCHAR(50)  DEFAULT NULL,
            gpio_pin      INT          DEFAULT NULL,
            esp32_mac     VARCHAR(17)  DEFAULT NULL,
            last_seen     TIMESTAMP    NULL,
            created_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE,
            INDEX idx_devices_room (room_id),
            INDEX idx_devices_sensor (is_sensor),
            INDEX idx_devices_last_seen (last_seen),
            INDEX idx_devices_mac (esp32_mac),
            INDEX idx_devices_type (type)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scenes (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            name        VARCHAR(100) NOT NULL,
            icon        VARCHAR(50)  DEFAULT 'mdi:play-circle',
            bg_image    VARCHAR(255) DEFAULT NULL,
            description VARCHAR(200) DEFAULT NULL,
            is_enabled  BOOLEAN      DEFAULT FALSE,
            created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_scenes_enabled (is_enabled)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scene_devices (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            scene_id     INT NOT NULL,
            device_id    INT NOT NULL,
            action_state ENUM('ON','OFF') NOT NULL,
            FOREIGN KEY (scene_id)  REFERENCES scenes(id)  ON DELETE CASCADE,
            FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
            INDEX idx_scene_devices_scene (scene_id),
            INDEX idx_scene_devices_device (device_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS automation_rules (
            id               INT AUTO_INCREMENT PRIMARY KEY,
            name             VARCHAR(100) NOT NULL,
            enabled          BOOLEAN      DEFAULT TRUE,
            trigger_type     VARCHAR(50)  NOT NULL,  -- sensor, time, schedule
            trigger_config   JSON         NOT NULL,
            condition_type   VARCHAR(50),
            condition_config JSON,
            action_device_id INT          NOT NULL,
            action_command   VARCHAR(50)  NOT NULL,
            action_parameters JSON,
            last_triggered   TIMESTAMP    NULL,
            created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (action_device_id) REFERENCES devices(id) ON DELETE CASCADE,
            INDEX idx_rules_enabled (enabled),
            INDEX idx_rules_trigger (trigger_type)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS device_history (
            id         INT AUTO_INCREMENT PRIMARY KEY,
            device_id  INT         NOT NULL,
            event_type VARCHAR(50) NOT NULL,
            event_data JSON,
            created_at TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
            INDEX idx_history_device (device_id),
            INDEX idx_history_created (created_at)
        )
    """)

    # ===== NEW AGRICULTURE TABLES =====
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS agricultural_zones (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            zone_name   VARCHAR(50) NOT NULL,
            zone_type   ENUM('irrigation', 'livestock', 'greenhouse') NOT NULL,
            room_id     INT,  -- Link to rooms table
            crop_type   VARCHAR(50),
            area_size   FLOAT,  -- in square meters
            is_active   BOOLEAN DEFAULT 1,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE SET NULL,
            INDEX idx_agri_zone_type (zone_type)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS water_systems (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            zone_id         INT,
            system_type     ENUM('irrigation_pump', 'livestock_water_pump', 'water_tank') NOT NULL,
            pump_state      BOOLEAN DEFAULT 0,
            water_level     INT DEFAULT 0,
            flow_rate       FLOAT DEFAULT 0,
            pressure        FLOAT DEFAULT 0,
            auto_mode       BOOLEAN DEFAULT 1,
            schedule_start  TIME,
            schedule_stop   TIME,
            last_activated  TIMESTAMP NULL,
            FOREIGN KEY (zone_id) REFERENCES agricultural_zones(id) ON DELETE CASCADE,
            INDEX idx_water_system_type (system_type)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS moisture_sensors (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            zone_id      INT,
            sensor_type  ENUM('soil_moisture', 'water_level') NOT NULL,
            sensor_value INT DEFAULT 0,
            battery_level INT DEFAULT 100,
            calibration_min INT DEFAULT 0,
            calibration_max INT DEFAULT 100,
            last_reading TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (zone_id) REFERENCES agricultural_zones(id) ON DELETE CASCADE
        )
    """)

    # ===== INDUSTRIAL IoT TABLES =====
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS industrial_sensors (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            sensor_name  VARCHAR(50) NOT NULL,
            sensor_type  ENUM('temperature', 'humidity', 'pressure', 'vibration', 'current') NOT NULL,
            location     VARCHAR(100),
            machine_id   VARCHAR(50),
            current_value FLOAT,
            unit         VARCHAR(10),
            alert_min    FLOAT,
            alert_max    FLOAT,
            is_alerting  BOOLEAN DEFAULT 0,
            last_reading TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_industrial_type (sensor_type),
            INDEX idx_industrial_machine (machine_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS industrial_alerts (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            sensor_id    INT,
            alert_type   VARCHAR(50),
            alert_value  FLOAT,
            threshold    FLOAT,
            acknowledged BOOLEAN DEFAULT 0,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sensor_id) REFERENCES industrial_sensors(id) ON DELETE CASCADE,
            INDEX idx_alerts_ack (acknowledged)
        )
    """)

    # ===== SMART CITY TABLES =====
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS street_lights (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            light_id        VARCHAR(20) UNIQUE NOT NULL,
            location_name   VARCHAR(100),
            latitude        FLOAT,
            longitude       FLOAT,
            intensity       INT DEFAULT 0,
            is_on           BOOLEAN DEFAULT 0,
            motion_detected BOOLEAN DEFAULT 0,
            auto_mode       BOOLEAN DEFAULT 1,
            power_consumption FLOAT DEFAULT 0,
            last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_street_lights_location (location_name)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS smart_bins (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            bin_id          VARCHAR(20) UNIQUE NOT NULL,
            location_name   VARCHAR(100),
            latitude        FLOAT,
            longitude       FLOAT,
            fill_level      INT DEFAULT 0,
            temperature     FLOAT,
            last_emptied    TIMESTAMP NULL,
            alert_threshold INT DEFAULT 80,
            battery_level   INT DEFAULT 100,
            last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_bins_fill (fill_level)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bin_collection_schedule (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            bin_id       VARCHAR(20),
            scheduled_date DATE,
            scheduled_time TIME,
            status       ENUM('pending', 'completed', 'missed') DEFAULT 'pending',
            completed_at TIMESTAMP NULL,
            FOREIGN KEY (bin_id) REFERENCES smart_bins(bin_id) ON DELETE CASCADE
        )
    """)

    # ===== OFFICE AUTOMATION TABLES =====
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS office_zones (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            zone_name   VARCHAR(50) NOT NULL,
            floor       INT,
            department  VARCHAR(50),
            room_id     INT,
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE SET NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS office_lighting (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            zone_id         INT,
            light_id        VARCHAR(20),
            is_on           BOOLEAN DEFAULT 0,
            brightness      INT DEFAULT 100,
            color_temp      INT DEFAULT 4000,  -- Kelvin
            occupancy_detected BOOLEAN DEFAULT 0,
            auto_mode       BOOLEAN DEFAULT 1,
            last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (zone_id) REFERENCES office_zones(id) ON DELETE CASCADE,
            INDEX idx_office_lighting_zone (zone_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ventilation_systems (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            zone_id     INT,
            fan_speed   INT DEFAULT 0,
            target_temp FLOAT,
            current_temp FLOAT,
            is_running  BOOLEAN DEFAULT 0,
            auto_mode   BOOLEAN DEFAULT 1,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (zone_id) REFERENCES office_zones(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS access_control (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            user_id      VARCHAR(50) UNIQUE NOT NULL,
            user_name    VARCHAR(100),
            has_access   BOOLEAN DEFAULT 0,
            access_level ENUM('admin', 'employee', 'visitor') DEFAULT 'employee',
            face_encoding TEXT,
            card_rfid    VARCHAR(50),
            last_access  TIMESTAMP NULL,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_access_user (user_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS access_logs (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            user_id      VARCHAR(50),
            access_point VARCHAR(50),
            granted      BOOLEAN,
            image_capture TEXT,
            timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_access_logs_user (user_id),
            INDEX idx_access_logs_time (timestamp)
        )
    """)

    # ===== ENERGY MANAGEMENT TABLES =====
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS energy_meters (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            meter_id     VARCHAR(50) UNIQUE NOT NULL,
            location     VARCHAR(100),
            meter_type   ENUM('main', 'sub', 'solar', 'ev') DEFAULT 'sub',
            current_power FLOAT DEFAULT 0,
            voltage      FLOAT DEFAULT 0,
            current      FLOAT DEFAULT 0,
            power_factor FLOAT DEFAULT 1,
            last_reading TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS energy_readings (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            meter_id     VARCHAR(50),
            power        FLOAT,
            energy_kwh   FLOAT,
            reading_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (meter_id) REFERENCES energy_meters(meter_id) ON DELETE CASCADE,
            INDEX idx_energy_readings_time (reading_time)
        )
    """)

    # ===== WEATHER STATION =====
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            temperature  FLOAT,
            humidity     FLOAT,
            pressure     FLOAT,
            wind_speed   FLOAT,
            wind_direction INT,
            rainfall     FLOAT,
            light_level  INT,
            recorded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_weather_time (recorded_at)
        )
    """)

    conn.commit()
    log.info("✅ Complete database schema initialized with all tables")

    # ===== MIGRATIONS FOR EXISTING TABLES =====
    
    # Add new columns to existing tables if they don't exist
    def _col_exists(cur, table: str, col: str) -> bool:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s",
            (DB_CONFIG["database"], table, col),
        )
        return cur.fetchone()[0] > 0

    def _add_col(cur, table: str, col: str, definition: str):
        if not _col_exists(cur, table, col):
            cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{col}` {definition}")
            log.info(f"Migration: added '{col}' to '{table}'")

    # Add room_type to rooms
    _add_col(cur, "rooms", "room_type", "VARCHAR(50) DEFAULT 'home'")
    
    # Add indexes for performance
    cur.execute("CREATE INDEX IF NOT EXISTS idx_devices_type ON devices(type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_history_created ON device_history(created_at)")
    
    conn.commit()
    cur.close()
    conn.close()
    log.info("✅ Database ready with all tables and indexes")


# Initialize database on startup
try:
    init_database()
except Exception as e:
    log.warning(f"DB init warning: {e}")

# ══════════════════════════════════════════════════════════════
#  LOOKUP TABLES & CONSTANTS (Cached)
# ══════════════════════════════════════════════════════════════

# All device types across all systems
DEVICE_TYPES = {
    # Home automation
    "home": ["light", "fan", "socket", "ac", "thermostat", "motion_sensor", 
             "door_sensor", "window_sensor", "smoke_sensor", "water_sensor", 
             "camera", "lock", "temperature", "humidity", "curtains", "speaker", "tv"],
    
    # Agriculture
    "agriculture": ["irrigation_pump", "livestock_pump", "soil_moisture_sensor", 
                    "water_tank_sensor", "rain_sensor", "wind_sensor"],
    
    # Industrial
    "industrial": ["industrial_temp", "industrial_humidity", "vibration_sensor", 
                   "pressure_sensor", "current_sensor", "leak_detector"],
    
    # Smart City
    "city": ["street_light", "smart_bin", "traffic_sensor", "parking_sensor", 
             "air_quality_sensor", "noise_sensor"],
    
    # Office
    "office": ["office_light", "occupancy_sensor", "access_reader", "ventilation_fan"]
}

# Flatten for validation
ALL_DEVICE_TYPES = [d for sublist in DEVICE_TYPES.values() for d in sublist]

ICON_MAP = {
    # Home
    "light": "mdi:lightbulb",
    "fan": "mdi:fan",
    "socket": "mdi:power-socket",
    "ac": "mdi:air-conditioner",
    "thermostat": "mdi:thermostat",
    "motion_sensor": "mdi:motion-sensor",
    "door_sensor": "mdi:door",
    "window_sensor": "mdi:window-open",
    "smoke_sensor": "mdi:smoke-detector",
    "water_sensor": "mdi:water",
    "camera": "mdi:cctv",
    "lock": "mdi:lock",
    "temperature": "mdi:thermometer",
    "humidity": "mdi:water-percent",
    "curtains": "mdi:curtains",
    "speaker": "mdi:speaker",
    "tv": "mdi:television",
    
    # Agriculture
    "irrigation_pump": "mdi:water-pump",
    "livestock_pump": "mdi:water-pump",
    "soil_moisture_sensor": "mdi:water-percent",
    "water_tank_sensor": "mdi:water",
    "rain_sensor": "mdi:weather-rainy",
    "wind_sensor": "mdi:weather-windy",
    
    # Industrial
    "industrial_temp": "mdi:thermometer-high",
    "industrial_humidity": "mdi:water-percent",
    "vibration_sensor": "mdi:vibrate",
    "pressure_sensor": "mdi:gauge",
    "current_sensor": "mdi:flash",
    "leak_detector": "mdi:leak",
    
    # City
    "street_light": "mdi:street-light",
    "smart_bin": "mdi:trash-can",
    "traffic_sensor": "mdi:traffic-light",
    "parking_sensor": "mdi:parking",
    "air_quality_sensor": "mdi:air-filter",
    "noise_sensor": "mdi:volume-high",
    
    # Office
    "office_light": "mdi:lightbulb-on",
    "occupancy_sensor": "mdi:account-group",
    "access_reader": "mdi:credit-card-lock",
    "ventilation_fan": "mdi:fan",
}

SENSOR_ICON_MAP = [
    ("temperature", "mdi:thermometer"),
    ("motion", "mdi:motion-sensor"),
    ("door", "mdi:door"),
    ("window", "mdi:window-open"),
    ("smoke", "mdi:smoke-detector"),
    ("water", "mdi:water"),
    ("gas", "mdi:gas-cylinder"),
    ("fire", "mdi:fire-alert"),
    ("humidity", "mdi:water-percent"),
    ("soil", "mdi:flower"),
    ("moisture", "mdi:water-percent"),
    ("rain", "mdi:weather-rainy"),
    ("wind", "mdi:weather-windy"),
    ("vibration", "mdi:vibrate"),
    ("pressure", "mdi:gauge"),
    ("current", "mdi:flash"),
    ("air", "mdi:air-filter"),
    ("noise", "mdi:volume-high"),
    ("occupancy", "mdi:account-group"),
]

ROOM_TYPE_MAP = {
    "home": "mdi:home",
    "agriculture": "mdi:agriculture",
    "industrial": "mdi:factory",
    "city": "mdi:city",
    "office": "mdi:office-building"
}


@lru_cache(maxsize=128)
def device_icon(device_type: str, is_sensor: bool = False) -> str:
    """Cached icon lookup - much faster"""
    t = device_type.lower()
    if is_sensor:
        for kw, icon in SENSOR_ICON_MAP:
            if kw in t:
                return icon
        return "mdi:sensor"
    return ICON_MAP.get(t, "mdi:devices")


@lru_cache(maxsize=64)
def scene_icon(name: str) -> str:
    n = name.lower()
    scene_icons = {
        "morning": "mdi:weather-sunny",
        "coffee": "mdi:coffee",
        "dinner": "mdi:silverware-fork-knife",
        "night": "mdi:moon-waning-crescent",
        "party": "mdi:party-popper",
        "away": "mdi:airplane",
        "irrigation": "mdi:water-pump",
        "feeding": "mdi:food",
        "lighting": "mdi:lightbulb-group",
        "security": "mdi:shield",
    }
    for key, icon in scene_icons.items():
        if key in n:
            return icon
    return "mdi:play-circle"


# ══════════════════════════════════════════════════════════════
#  PYDANTIC SCHEMAS - ALL MODELS
# ══════════════════════════════════════════════════════════════

# ===== Base Models =====

class RoomCreate(BaseModel):
    name: str
    icon: Optional[str] = "mdi:door"
    room_type: Optional[str] = "home"


class RoomResponse(BaseModel):
    id: int
    name: str
    icon: Optional[str] = "mdi:door"
    room_type: Optional[str] = "home"
    created_at: Optional[datetime] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class DeviceCreate(BaseModel):
    name: str
    type: str
    room_id: int
    is_sensor: bool = False
    sensor_value: Optional[str] = None
    icon: Optional[str] = None
    gpio_pin: Optional[int] = None
    esp32_mac: Optional[str] = None

    @validator("type")
    def validate_type(cls, v):
        if v not in ALL_DEVICE_TYPES:
            raise ValueError(f"Device type must be one of: {', '.join(ALL_DEVICE_TYPES[:10])}...")
        return v


class DeviceResponse(BaseModel):
    id: int
    name: str
    type: str
    room_id: int
    current_state: str
    is_sensor: bool
    sensor_value: Optional[str] = None
    icon: Optional[str] = None
    last_seen: Optional[datetime] = None
    created_at: Optional[datetime] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class SceneCreate(BaseModel):
    name: str
    icon: Optional[str] = None
    bg_image: Optional[str] = None
    description: Optional[str] = None
    is_enabled: bool = False


class SceneResponse(BaseModel):
    id: int
    name: str
    icon: Optional[str]
    bg_image: Optional[str]
    description: Optional[str]
    is_enabled: bool
    created_at: Optional[datetime] = None


class SceneDeviceItem(BaseModel):
    device_id: int
    action_state: str

    @validator("action_state")
    def validate_state(cls, v):
        if v.upper() not in ("ON", "OFF"):
            raise ValueError("action_state must be 'ON' or 'OFF'")
        return v.upper()


class SceneDeviceCreate(BaseModel):
    devices: List[SceneDeviceItem]


class SensorUpdatePayload(BaseModel):
    sensor_value: str


# ===== Agriculture Models =====

class AgriculturalZoneCreate(BaseModel):
    zone_name: str
    zone_type: str  # irrigation, livestock, greenhouse
    room_id: Optional[int] = None
    crop_type: Optional[str] = None
    area_size: Optional[float] = None


class WaterSystemStatus(BaseModel):
    pump_id: int
    pump_name: str
    pump_state: bool
    auto_mode: bool
    water_level: Optional[int] = None
    last_activated: Optional[datetime] = None


class MoistureSensorData(BaseModel):
    sensor_id: int
    value: int
    battery: Optional[int] = 100


class ThresholdRule(BaseModel):
    name: str
    sensor_id: int
    threshold: float
    condition: str = "<"  # <, >, <=, >=, ==
    action_device_id: int
    action_command: str = "ON"
    enabled: bool = True


# ===== Industrial Models =====

class IndustrialSensorData(BaseModel):
    sensor_id: int
    value: float
    unit: str
    timestamp: Optional[datetime] = None


class IndustrialAlert(BaseModel):
    sensor_id: int
    alert_type: str
    value: float
    threshold: float


# ===== Smart City Models =====

class StreetLightControl(BaseModel):
    light_id: str
    state: bool
    intensity: Optional[int] = 100
    auto_mode: Optional[bool] = True


class SmartBinData(BaseModel):
    bin_id: str
    fill_level: int
    temperature: Optional[float] = None
    battery: Optional[int] = None


class BinCollectionSchedule(BaseModel):
    bin_id: str
    scheduled_date: str
    scheduled_time: str


# ===== Office Automation Models =====

class OfficeZoneCreate(BaseModel):
    zone_name: str
    floor: Optional[int] = None
    department: Optional[str] = None
    room_id: Optional[int] = None


class OfficeLightingControl(BaseModel):
    zone_id: int
    light_id: str
    state: bool
    brightness: Optional[int] = 100
    auto_mode: Optional[bool] = True


class VentilationControl(BaseModel):
    zone_id: int
    fan_speed: int  # 0-100
    auto_mode: Optional[bool] = True


class AccessRequest(BaseModel):
    user_id: str
    access_point: str
    image: Optional[str] = None  # base64 encoded


class AccessGrant(BaseModel):
    user_id: str
    user_name: str
    access_level: str = "employee"
    card_rfid: Optional[str] = None


# ===== ESP32 Models =====

class ESP32Heartbeat(BaseModel):
    device_id: int
    ip_address: str
    mac_address: str
    firmware_version: str
    rssi: Optional[int] = None
    free_heap: Optional[int] = None
    uptime: Optional[int] = None


class ESP32SensorData(BaseModel):
    device_id: int
    sensor_type: str
    value: float
    unit: str
    timestamp: Optional[datetime] = None

    @validator("timestamp", pre=True, always=True)
    def set_timestamp(cls, v):
        return v or datetime.now()


class ESP32RegisterBody(BaseModel):
    device_id: int
    mac_address: str
    ip_address: str


# ===== Automation Models =====

class AutomationRuleCreate(BaseModel):
    name: str
    enabled: bool = True
    trigger_type: str  # sensor, time, schedule
    trigger_config: Dict[str, Any]
    condition_type: Optional[str] = None
    condition_config: Optional[Dict[str, Any]] = None
    action_device_id: int
    action_command: str
    action_parameters: Optional[Dict[str, Any]] = None


# ===== Energy Models =====

class EnergyReading(BaseModel):
    meter_id: str
    power: float
    energy_kwh: float


# ===== Weather Models =====

class WeatherData(BaseModel):
    temperature: float
    humidity: float
    pressure: Optional[float] = None
    wind_speed: Optional[float] = None
    wind_direction: Optional[int] = None
    rainfall: Optional[float] = None
    light_level: Optional[int] = None


# ══════════════════════════════════════════════════════════════
#  FASTAPI APP INITIALIZATION
# ══════════════════════════════════════════════════════════════

app = FastAPI(
    title="UNIFIED IoT CONTROL SYSTEM",
    description="Complete IoT backend for Smart Home, Agriculture, Industrial, Smart City, and Office Automation",
    version="5.0.0-UNIFIED",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add compression middleware
app.add_middleware(GZipMiddleware, minimum_size=500)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Response time tracking middleware
@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    
    # Log slow requests (>200ms) for debugging
    if process_time > 0.2:
        log.warning(f"Slow request: {request.method} {request.url.path} - {process_time:.3f}s")
    
    return response


# Cache control for static-like endpoints
@app.middleware("http")
async def cache_control_middleware(request, call_next):
    response = await call_next(request)
    if request.method == "GET" and not request.url.path.startswith("/api/esp32"):
        # Cache GET responses for 2 seconds
        response.headers["Cache-Control"] = "max-age=2"
    return response


# ══════════════════════════════════════════════════════════════
#  ROOMS ENDPOINTS (Enhanced)
# ══════════════════════════════════════════════════════════════

@app.get("/api/rooms", response_model=List[RoomResponse], tags=["Rooms"])
def get_rooms(room_type: Optional[str] = None):
    """Get all rooms, optionally filtered by type"""
    cache_key = f"rooms_{room_type}" if room_type else "all_rooms"
    
    def _get_rooms():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        if room_type:
            cur.execute("SELECT id, name, icon, room_type, created_at FROM rooms WHERE room_type=%s ORDER BY id", (room_type,))
        else:
            cur.execute("SELECT id, name, icon, room_type, created_at FROM rooms ORDER BY id")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    
    return get_cached_or_set(cache_key, _get_rooms)


@app.get("/api/rooms/{room_id}", tags=["Rooms"])
def get_room(room_id: int):
    cache_key = f"room_{room_id}"
    
    def _get_room():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id, name, icon, room_type FROM rooms WHERE id=%s", (room_id,))
        room = cur.fetchone()
        if not room:
            cur.close()
            conn.close()
            raise HTTPException(404, "Room not found")
        cur.execute(
            "SELECT id, name, type, current_state, is_sensor, sensor_value, icon, last_seen "
            "FROM devices WHERE room_id=%s ORDER BY is_sensor ASC, name ASC",
            (room_id,),
        )
        devices = cur.fetchall()
        cur.close()
        conn.close()
        return {
            "id": room["id"], 
            "name": room["name"], 
            "icon": room["icon"],
            "room_type": room["room_type"],
            "devices": devices
        }
    
    try:
        return get_cached_or_set(cache_key, _get_room)
    except HTTPException:
        raise


@app.get("/api/rooms-with-devices", tags=["Rooms"])
def get_rooms_with_devices(room_type: Optional[str] = None):
    """Get all rooms with their devices"""
    cache_key = f"rooms_with_devices_{room_type}" if room_type else "rooms_with_devices"
    
    def _get_rooms_with_devices():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        
        query = """
            SELECT r.id, r.name, r.icon, r.room_type,
                   d.id AS device_id, d.name AS device_name, d.type,
                   d.current_state, d.is_sensor, d.sensor_value, d.icon AS device_icon,
                   d.last_seen
            FROM rooms r
            LEFT JOIN devices d ON r.id = d.room_id
        """
        
        if room_type:
            query += " WHERE r.room_type = %s"
            cur.execute(query + " ORDER BY r.id, d.is_sensor ASC, d.name ASC", (room_type,))
        else:
            cur.execute(query + " ORDER BY r.id, d.is_sensor ASC, d.name ASC")
            
        rows = cur.fetchall()
        cur.close()
        conn.close()

        rooms: Dict[int, Any] = {}
        for row in rows:
            rid = row["id"]
            if rid not in rooms:
                rooms[rid] = {
                    "id": rid, 
                    "name": row["name"], 
                    "icon": row["icon"],
                    "room_type": row["room_type"],
                    "devices": []
                }
            if row["device_id"]:
                rooms[rid]["devices"].append({
                    "id": row["device_id"],
                    "name": row["device_name"],
                    "type": row["type"],
                    "state": row["current_state"],
                    "is_sensor": bool(row["is_sensor"]),
                    "sensor_value": row["sensor_value"],
                    "icon": row["device_icon"] or device_icon(row["type"], row["is_sensor"]),
                    "last_seen": row["last_seen"].isoformat() if row["last_seen"] else None
                })
        return list(rooms.values())
    
    return get_cached_or_set(cache_key, _get_rooms_with_devices)


@app.post("/api/rooms", response_model=RoomResponse, status_code=201, tags=["Rooms"])
def create_room(room: RoomCreate):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id FROM rooms WHERE name=%s", (room.name,))
    if cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(400, "Room name already exists")
    
    cur.execute(
        "INSERT INTO rooms (name, icon, room_type) VALUES (%s, %s, %s)", 
        (room.name, room.icon, room.room_type)
    )
    conn.commit()
    
    cur.execute("SELECT id, name, icon, room_type, created_at FROM rooms WHERE id=%s", (cur.lastrowid,))
    new = cur.fetchone()
    cur.close()
    conn.close()
    
    # Invalidate cache
    cache.pop("all_rooms", None)
    cache.pop("rooms_with_devices", None)
    cache.pop(f"rooms_{room.room_type}", None)
    
    return new


@app.put("/api/rooms/{room_id}", tags=["Rooms"])
def update_room(room_id: int, room: RoomCreate):
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute(
        "UPDATE rooms SET name=%s, icon=%s, room_type=%s WHERE id=%s",
        (room.name, room.icon, room.room_type, room_id)
    )
    conn.commit()
    affected = cur.rowcount
    cur.close()
    conn.close()
    
    if affected == 0:
        raise HTTPException(404, "Room not found")
    
    # Invalidate cache
    cache.pop(f"room_{room_id}", None)
    cache.pop("all_rooms", None)
    cache.pop("rooms_with_devices", None)
    cache.pop(f"rooms_{room.room_type}", None)
    
    return {"message": "Room updated"}


@app.delete("/api/rooms/{room_id}", tags=["Rooms"])
def delete_room(room_id: int):
    conn = get_db()
    cur = conn.cursor()
    
    # Get room type for cache invalidation
    cur.execute("SELECT room_type FROM rooms WHERE id=%s", (room_id,))
    result = cur.fetchone()
    room_type = result[0] if result else None
    
    cur.execute("DELETE FROM rooms WHERE id=%s", (room_id,))
    conn.commit()
    cur.close()
    conn.close()
    
    # Invalidate cache
    cache.pop("all_rooms", None)
    cache.pop("rooms_with_devices", None)
    cache.pop(f"room_{room_id}", None)
    if room_type:
        cache.pop(f"rooms_{room_type}", None)
    
    return {"message": "Room deleted"}


# ══════════════════════════════════════════════════════════════
#  DEVICES ENDPOINTS (Enhanced)
# ══════════════════════════════════════════════════════════════

@app.get("/api/devices", response_model=List[DeviceResponse], tags=["Devices"])
def get_devices(device_type: Optional[str] = None, room_id: Optional[int] = None):
    """Get all devices, optionally filtered by type or room"""
    cache_key = f"devices_{device_type}_{room_id}" if device_type or room_id else "all_devices"
    
    def _get_devices():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        
        query = """
            SELECT id, name, type, room_id, current_state, is_sensor, 
                   sensor_value, icon, last_seen, created_at 
            FROM devices 
            WHERE 1=1
        """
        params = []
        
        if device_type:
            query += " AND type = %s"
            params.append(device_type)
        
        if room_id:
            query += " AND room_id = %s"
            params.append(room_id)
        
        query += " ORDER BY room_id, is_sensor ASC, name"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    
    return get_cached_or_set(cache_key, _get_devices)


@app.get("/api/devices/room/{room_id}", tags=["Devices"])
def get_devices_by_room(room_id: int):
    cache_key = f"devices_room_{room_id}"
    
    def _get_devices():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, name, type, current_state, is_sensor, sensor_value, icon, last_seen "
            "FROM devices WHERE room_id=%s ORDER BY is_sensor ASC, name",
            (room_id,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    
    return get_cached_or_set(cache_key, _get_devices)


@app.get("/api/devices/{device_id}", tags=["Devices"])
def get_device(device_id: int):
    cache_key = f"device_{device_id}"
    
    def _get_device():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, name, type, room_id, current_state, is_sensor, "
            "sensor_value, icon, last_seen, created_at FROM devices WHERE id=%s",
            (device_id,),
        )
        device = cur.fetchone()
        cur.close()
        conn.close()
        if not device:
            raise HTTPException(404, "Device not found")
        return device
    
    try:
        return get_cached_or_set(cache_key, _get_device)
    except HTTPException:
        raise


@app.post("/api/devices", response_model=DeviceResponse, status_code=201, tags=["Devices"])
def create_device(device: DeviceCreate):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    
    # Verify room exists
    cur.execute("SELECT id FROM rooms WHERE id=%s", (device.room_id,))
    if not cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(404, "Room not found")
    
    icon = device.icon or device_icon(device.type, device.is_sensor)
    
    cur.execute(
        """INSERT INTO devices 
           (name, type, room_id, current_state, is_sensor, sensor_value, icon, gpio_pin, esp32_mac) 
           VALUES (%s,%s,%s,'OFF',%s,%s,%s,%s,%s)""",
        (device.name, device.type, device.room_id, device.is_sensor,
         device.sensor_value, icon, device.gpio_pin, device.esp32_mac),
    )
    conn.commit()
    
    cur.execute(
        "SELECT id, name, type, room_id, current_state, is_sensor, sensor_value, icon, created_at "
        "FROM devices WHERE id=%s",
        (cur.lastrowid,),
    )
    new = cur.fetchone()
    cur.close()
    conn.close()
    
    # Invalidate cache
    cache.pop("all_devices", None)
    cache.pop(f"devices_room_{device.room_id}", None)
    cache.pop("rooms_with_devices", None)
    
    return new


@app.put("/api/devices/{device_id}/toggle", tags=["Devices"])
def toggle_device(device_id: int):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    
    cur.execute("SELECT id, current_state, is_sensor, room_id FROM devices WHERE id=%s", (device_id,))
    dev = cur.fetchone()
    if not dev:
        cur.close()
        conn.close()
        raise HTTPException(404, "Device not found")
    
    if dev["is_sensor"]:
        cur.close()
        conn.close()
        raise HTTPException(400, "Cannot toggle a sensor device")
    
    new_state = "OFF" if dev["current_state"] == "ON" else "ON"
    cur.execute("UPDATE devices SET current_state=%s WHERE id=%s", (new_state, device_id))
    
    # Log to history
    cur.execute(
        "INSERT INTO device_history (device_id, event_type, event_data) VALUES (%s, 'toggle', %s)",
        (device_id, json.dumps({"new_state": new_state}))
    )
    
    conn.commit()
    cur.close()
    conn.close()
    
    # Invalidate cache
    cache.pop(f"device_{device_id}", None)
    cache.pop(f"devices_room_{dev['room_id']}", None)
    cache.pop("all_devices", None)
    cache.pop("rooms_with_devices", None)
    
    return {"id": device_id, "state": new_state}


@app.put("/api/devices/{device_id}/state", tags=["Devices"])
def set_device_state(device_id: int, state_data: dict):
    """Set device to specific state (ON/OFF)"""
    state = state_data.get("state", "OFF").upper()
    if state not in ("ON", "OFF"):
        raise HTTPException(400, "State must be ON or OFF")
    
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    
    cur.execute("SELECT id, is_sensor, room_id FROM devices WHERE id=%s", (device_id,))
    dev = cur.fetchone()
    if not dev:
        cur.close()
        conn.close()
        raise HTTPException(404, "Device not found")
    
    if dev["is_sensor"]:
        cur.close()
        conn.close()
        raise HTTPException(400, "Cannot set state of a sensor device")
    
    cur.execute("UPDATE devices SET current_state=%s WHERE id=%s", (state, device_id))
    
    # Log to history
    cur.execute(
        "INSERT INTO device_history (device_id, event_type, event_data) VALUES (%s, 'set_state', %s)",
        (device_id, json.dumps({"state": state}))
    )
    
    conn.commit()
    cur.close()
    conn.close()
    
    # Invalidate cache
    cache.pop(f"device_{device_id}", None)
    cache.pop(f"devices_room_{dev['room_id']}", None)
    cache.pop("all_devices", None)
    cache.pop("rooms_with_devices", None)
    
    return {"id": device_id, "state": state}


@app.put("/api/devices/{device_id}/sensor", tags=["Devices"])
def update_sensor_value(device_id: int, payload: SensorUpdatePayload):
    """
    Called by ESP32 nodes to push live sensor readings.
    Optimized fast path with minimal overhead
    """
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        # Fast check - only need ID and room_id
        cur.execute("SELECT id, room_id FROM devices WHERE id=%s", (device_id,))
        dev = cur.fetchone()
        if not dev:
            raise HTTPException(404, "Device not found")

        # Fast update
        cur.execute(
            "UPDATE devices SET sensor_value=%s, last_seen=NOW() WHERE id=%s",
            (payload.sensor_value, device_id),
        )
        conn.commit()
        
        # Only log every 10th update to reduce noise
        if random.randint(1, 10) == 1:
            log.info(f"[Sensor] device {device_id} → {payload.sensor_value}")

        # Trigger automation checks (optimized)
        _check_automation_fast(cur, conn, device_id, payload.sensor_value)

        # Return minimal response
        return {"status": "ok", "t": int(time.time())}
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Sensor update error: {e}")
        raise HTTPException(500, "Update failed")
    finally:
        cur.close()
        conn.close()


@app.delete("/api/devices/{device_id}", tags=["Devices"])
def delete_device(device_id: int):
    conn = get_db()
    cur = conn.cursor()
    
    # Get room_id first for cache invalidation
    cur.execute("SELECT room_id FROM devices WHERE id=%s", (device_id,))
    result = cur.fetchone()
    room_id = result[0] if result else None
    
    cur.execute("DELETE FROM devices WHERE id=%s", (device_id,))
    conn.commit()
    cur.close()
    conn.close()
    
    # Invalidate cache
    cache.pop(f"device_{device_id}", None)
    if room_id:
        cache.pop(f"devices_room_{room_id}", None)
    cache.pop("all_devices", None)
    cache.pop("rooms_with_devices", None)
    
    return {"message": "Device deleted"}


@app.get("/api/device-types", tags=["Devices"])
def get_device_types():
    """Get all available device types grouped by category"""
    cache_key = "device_types"
    
    def _get_types():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT DISTINCT type FROM devices ORDER BY type")
        used = [r["type"] for r in cur.fetchall()]
        cur.close()
        conn.close()
        
        return {
            "by_category": DEVICE_TYPES,
            "used": used,
            "all": sorted(set(ALL_DEVICE_TYPES + used))
        }
    
    return get_cached_or_set(cache_key, _get_types)


# ══════════════════════════════════════════════════════════════
#  SCENES ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.get("/api/scenes", tags=["Scenes"])
def get_scenes():
    cache_key = "all_scenes"
    
    def _get_scenes():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id, name, icon, bg_image, description, is_enabled, created_at FROM scenes ORDER BY id")
        scenes = cur.fetchall()
        
        cur.execute("""
            SELECT sd.scene_id, sd.device_id, sd.action_state,
                   d.name, d.type, d.is_sensor, d.icon AS device_icon
            FROM scene_devices sd
            JOIN devices d ON sd.device_id = d.id
            ORDER BY sd.scene_id, d.name
        """)
        scene_devs = cur.fetchall()
        cur.close()
        conn.close()

        by_scene: Dict[int, list] = {}
        for sd in scene_devs:
            sid = sd["scene_id"]
            by_scene.setdefault(sid, []).append({
                "device_id": sd["device_id"],
                "name": sd["name"],
                "type": sd["type"],
                "action_state": sd["action_state"],
                "is_sensor": bool(sd["is_sensor"]),
                "icon": sd["device_icon"] or device_icon(sd["type"], sd["is_sensor"]),
            })

        return [{
            "id": s["id"],
            "name": s["name"],
            "icon": s["icon"] or scene_icon(s["name"]),
            "bg_image": s["bg_image"],
            "description": s["description"],
            "is_enabled": bool(s["is_enabled"]),
            "device_count": len(by_scene.get(s["id"], [])),
            "devices": by_scene.get(s["id"], []),
        } for s in scenes]
    
    return get_cached_or_set(cache_key, _get_scenes)


@app.get("/api/scenes/{scene_id}", tags=["Scenes"])
def get_scene(scene_id: int):
    cache_key = f"scene_{scene_id}"
    
    def _get_scene():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, name, icon, bg_image, description, is_enabled, created_at "
            "FROM scenes WHERE id=%s", (scene_id,),
        )
        s = cur.fetchone()
        if not s:
            cur.close()
            conn.close()
            raise HTTPException(404, "Scene not found")
        
        cur.execute("""
            SELECT sd.device_id, d.name, d.type, sd.action_state, d.is_sensor, d.icon AS device_icon
            FROM scene_devices sd JOIN devices d ON sd.device_id=d.id
            WHERE sd.scene_id=%s
        """, (scene_id,))
        devs = cur.fetchall()
        cur.close()
        conn.close()
        
        return {
            "id": s["id"],
            "name": s["name"],
            "icon": s["icon"] or scene_icon(s["name"]),
            "bg_image": s["bg_image"],
            "description": s["description"],
            "is_enabled": bool(s["is_enabled"]),
            "devices": [{
                "device_id": d["device_id"],
                "name": d["name"],
                "type": d["type"],
                "action_state": d["action_state"],
                "is_sensor": bool(d["is_sensor"]),
                "icon": d["device_icon"] or device_icon(d["type"], d["is_sensor"]),
            } for d in devs],
        }
    
    try:
        return get_cached_or_set(cache_key, _get_scene)
    except HTTPException:
        raise


@app.post("/api/scenes", response_model=SceneResponse, status_code=201, tags=["Scenes"])
def create_scene(scene: SceneCreate):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "INSERT INTO scenes (name, icon, bg_image, description, is_enabled) VALUES (%s,%s,%s,%s,%s)",
        (scene.name, scene.icon or scene_icon(scene.name),
         scene.bg_image, scene.description, scene.is_enabled),
    )
    conn.commit()
    
    cur.execute(
        "SELECT id, name, icon, bg_image, description, is_enabled, created_at "
        "FROM scenes WHERE id=%s", (cur.lastrowid,),
    )
    new = cur.fetchone()
    cur.close()
    conn.close()
    
    cache.pop("all_scenes", None)
    return new


@app.post("/api/scenes/{scene_id}/devices", status_code=201, tags=["Scenes"])
def set_scene_devices(scene_id: int, payload: SceneDeviceCreate):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    
    cur.execute("SELECT id FROM scenes WHERE id=%s", (scene_id,))
    if not cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(404, "Scene not found")
    
    cur.execute("DELETE FROM scene_devices WHERE scene_id=%s", (scene_id,))
    
    for item in payload.devices:
        cur.execute("SELECT id, is_sensor FROM devices WHERE id=%s", (item.device_id,))
        dev = cur.fetchone()
        if not dev:
            cur.close()
            conn.close()
            raise HTTPException(404, f"Device {item.device_id} not found")
        if dev["is_sensor"]:
            cur.close()
            conn.close()
            raise HTTPException(400, "Sensor devices cannot be added to scenes")
        
        cur.execute(
            "INSERT INTO scene_devices (scene_id, device_id, action_state) VALUES (%s,%s,%s)",
            (scene_id, item.device_id, item.action_state),
        )
    
    conn.commit()
    cur.close()
    conn.close()
    
    cache.pop(f"scene_{scene_id}", None)
    cache.pop("all_scenes", None)
    
    return {"message": "Scene devices updated"}


@app.post("/api/scenes/{scene_id}/activate", tags=["Scenes"])
def activate_scene(scene_id: int):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, name FROM scenes WHERE id=%s", (scene_id,))
        scene = cur.fetchone()
        if not scene:
            raise HTTPException(404, "Scene not found")

        # Deactivate other active scenes
        cur.execute("SELECT id FROM scenes WHERE is_enabled=TRUE AND id!=%s", (scene_id,))
        prev_active = cur.fetchall()
        for prev in prev_active:
            cur.execute("UPDATE scenes SET is_enabled=FALSE WHERE id=%s", (prev["id"],))

        # Activate this scene
        cur.execute("UPDATE scenes SET is_enabled=TRUE WHERE id=%s", (scene_id,))
        
        # Apply scene device states
        cur.execute("SELECT device_id, action_state FROM scene_devices WHERE scene_id=%s", (scene_id,))
        scene_devs = cur.fetchall()
        for sd in scene_devs:
            cur.execute("UPDATE devices SET current_state=%s WHERE id=%s", (sd["action_state"], sd["device_id"]))

        conn.commit()
        
        # Invalidate cache
        cache.pop(f"scene_{scene_id}", None)
        cache.pop("all_scenes", None)
        cache.pop("all_devices", None)
        cache.pop("rooms_with_devices", None)

        return {
            "message": f"Scene '{scene['name']}' activated",
            "deactivated": len(prev_active),
            "devices_updated": len(scene_devs),
        }
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close()
        conn.close()


@app.post("/api/scenes/{scene_id}/deactivate", tags=["Scenes"])
def deactivate_scene(scene_id: int):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, name, is_enabled FROM scenes WHERE id=%s", (scene_id,))
        scene = cur.fetchone()
        if not scene:
            raise HTTPException(404, "Scene not found")
        
        if not scene["is_enabled"]:
            return {"message": f"Scene '{scene['name']}' was already inactive", "devices_turned_off": 0}
        
        cur.execute("UPDATE scenes SET is_enabled=FALSE WHERE id=%s", (scene_id,))
        
        # Turn off all devices in the scene
        cur.execute("SELECT device_id FROM scene_devices WHERE scene_id=%s", (scene_id,))
        devs = cur.fetchall()
        for d in devs:
            cur.execute("UPDATE devices SET current_state='OFF' WHERE id=%s", (d["device_id"],))
        
        conn.commit()
        
        cache.pop(f"scene_{scene_id}", None)
        cache.pop("all_scenes", None)
        
        return {"message": f"Scene '{scene['name']}' deactivated", "devices_turned_off": len(devs)}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close()
        conn.close()


@app.delete("/api/scenes/{scene_id}", tags=["Scenes"])
def delete_scene(scene_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM scenes WHERE id=%s", (scene_id,))
    conn.commit()
    cur.close()
    conn.close()
    
    cache.pop(f"scene_{scene_id}", None)
    cache.pop("all_scenes", None)
    
    return {"message": "Scene deleted"}


# ══════════════════════════════════════════════════════════════
#  AGRICULTURE ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.get("/api/agriculture/zones", tags=["Agriculture"])
def get_agricultural_zones():
    """Get all agricultural zones"""
    cache_key = "agri_zones"
    
    def _get_zones():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT az.*, r.name as room_name, r.icon as room_icon
            FROM agricultural_zones az
            LEFT JOIN rooms r ON az.room_id = r.id
            ORDER BY az.zone_type, az.zone_name
        """)
        zones = cur.fetchall()
        cur.close()
        conn.close()
        return zones
    
    return get_cached_or_set(cache_key, _get_zones)


@app.post("/api/agriculture/zones", status_code=201, tags=["Agriculture"])
def create_agricultural_zone(zone: AgriculturalZoneCreate):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    
    # Verify room if provided
    if zone.room_id:
        cur.execute("SELECT id FROM rooms WHERE id=%s", (zone.room_id,))
        if not cur.fetchone():
            cur.close()
            conn.close()
            raise HTTPException(404, "Room not found")
    
    cur.execute("""
        INSERT INTO agricultural_zones (zone_name, zone_type, room_id, crop_type, area_size)
        VALUES (%s, %s, %s, %s, %s)
    """, (zone.zone_name, zone.zone_type, zone.room_id, zone.crop_type, zone.area_size))
    
    zone_id = cur.lastrowid
    conn.commit()
    cur.close()
    conn.close()
    
    cache.pop("agri_zones", None)
    
    return {"id": zone_id, "message": "Agricultural zone created"}


@app.get("/api/agriculture/status", tags=["Agriculture"])
def get_agriculture_status():
    """Get complete agriculture and livestock status"""
    cache_key = "agriculture_status"
    
    def _get_status():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        
        # Get all agriculture-related devices
        agri_types = ['irrigation_pump', 'livestock_pump', 'water_sensor', 
                      'soil_moisture_sensor', 'rain_sensor', 'wind_sensor']
        
        placeholders = ','.join(['%s'] * len(agri_types))
        
        cur.execute(f"""
            SELECT d.*, r.name as room_name, r.room_type
            FROM devices d
            JOIN rooms r ON d.room_id = r.id
            WHERE d.type IN ({placeholders})
            ORDER BY 
                CASE 
                    WHEN d.type LIKE '%pump' THEN 1
                    WHEN d.type LIKE '%sensor' THEN 2
                    ELSE 3
                END,
                d.name
        """, agri_types)
        
        devices = cur.fetchall()
        
        # Get water systems status
        cur.execute("""
            SELECT ws.*, az.zone_name
            FROM water_systems ws
            JOIN agricultural_zones az ON ws.zone_id = az.id
        """)
        water_systems = cur.fetchall()
        
        # Get moisture sensors
        cur.execute("""
            SELECT ms.*, az.zone_name
            FROM moisture_sensors ms
            JOIN agricultural_zones az ON ms.zone_id = az.id
            ORDER BY ms.last_reading DESC
        """)
        moisture_sensors = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return {
            "devices": devices,
            "water_systems": water_systems,
            "moisture_sensors": moisture_sensors,
            "timestamp": datetime.now().isoformat()
        }
    
    return get_cached_or_set(cache_key, _get_status)


@app.get("/api/agriculture/irrigation", tags=["Agriculture"])
def get_irrigation_status():
    """Get irrigation-specific status"""
    cache_key = "irrigation_status"
    
    def _get_status():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        
        cur.execute("""
            SELECT d.*, r.name as room_name 
            FROM devices d
            JOIN rooms r ON d.room_id = r.id
            WHERE d.type IN ('irrigation_pump', 'soil_moisture_sensor')
            ORDER BY d.type
        """)
        
        devices = cur.fetchall()
        
        # Get automation rules for irrigation
        cur.execute("""
            SELECT * FROM automation_rules 
            WHERE trigger_type = 'sensor' 
            AND JSON_EXTRACT(trigger_config, '$.sensor_id') IS NOT NULL
            AND action_device_id IN (SELECT id FROM devices WHERE type = 'irrigation_pump')
        """)
        rules = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return {
            "devices": devices,
            "automation_rules": rules,
            "default_thresholds": {
                "min_moisture": 30,
                "max_moisture": 70
            }
        }
    
    return get_cached_or_set(cache_key, _get_status)


@app.get("/api/agriculture/livestock", tags=["Agriculture"])
def get_livestock_status():
    """Get livestock-specific status (water pump + water level)"""
    cache_key = "livestock_status"
    
    def _get_status():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        
        cur.execute("""
            SELECT d.*, r.name as room_name 
            FROM devices d
            JOIN rooms r ON d.room_id = r.id
            WHERE d.type IN ('livestock_pump', 'water_sensor')
            ORDER BY d.type
        """)
        
        devices = cur.fetchall()
        
        cur.execute("""
            SELECT * FROM automation_rules 
            WHERE trigger_type = 'sensor' 
            AND JSON_EXTRACT(trigger_config, '$.sensor_id') IN 
                (SELECT id FROM devices WHERE type = 'water_sensor')
        """)
        rules = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return {
            "devices": devices,
            "automation_rules": rules,
            "thresholds": {
                "min_water": 20,
                "max_water": 90
            }
        }
    
    return get_cached_or_set(cache_key, _get_status)


@app.post("/api/agriculture/threshold-rule", tags=["Agriculture"])
def create_agriculture_threshold_rule(rule: ThresholdRule):
    """Create automation rule for soil moisture or water level thresholds"""
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    
    try:
        # Verify devices exist
        cur.execute("SELECT id, type FROM devices WHERE id IN (%s, %s)", 
                   (rule.sensor_id, rule.action_device_id))
        devices = cur.fetchall()
        if len(devices) != 2:
            raise HTTPException(404, "Sensor or action device not found")
        
        # Create automation rule
        trigger_config = {
            "sensor_id": rule.sensor_id,
            "threshold": rule.threshold,
            "condition": rule.condition
        }
        
        cur.execute("""
            INSERT INTO automation_rules
            (name, enabled, trigger_type, trigger_config, action_device_id, action_command)
            VALUES (%s, %s, 'sensor', %s, %s, %s)
        """, (
            rule.name, rule.enabled, json.dumps(trigger_config),
            rule.action_device_id, rule.action_command
        ))
        
        rule_id = cur.lastrowid
        conn.commit()
        
        cache.pop("automation_rules", None)
        cache.pop("irrigation_status", None)
        cache.pop("livestock_status", None)
        
        return {"success": True, "rule_id": rule_id}
        
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close()
        conn.close()


# ══════════════════════════════════════════════════════════════
#  INDUSTRIAL IoT ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.get("/api/industrial/sensors", tags=["Industrial"])
def get_industrial_sensors():
    """Get all industrial sensors"""
    cache_key = "industrial_sensors"
    
    def _get_sensors():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM industrial_sensors ORDER BY machine_id, sensor_type")
        sensors = cur.fetchall()
        cur.close()
        conn.close()
        return sensors
    
    return get_cached_or_set(cache_key, _get_sensors)


@app.post("/api/industrial/sensor-data", tags=["Industrial"])
def receive_industrial_sensor_data(data: IndustrialSensorData):
    """Receive sensor data from industrial IoT devices"""
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    
    try:
        # Update sensor current value
        cur.execute("""
            UPDATE industrial_sensors 
            SET current_value=%s, last_reading=NOW() 
            WHERE id=%s
        """, (data.value, data.sensor_id))
        
        # Check for alerts
        cur.execute("SELECT alert_min, alert_max FROM industrial_sensors WHERE id=%s", (data.sensor_id,))
        sensor = cur.fetchone()
        
        alerts = []
        if sensor:
            if sensor["alert_min"] and data.value < sensor["alert_min"]:
                alerts.append({
                    "type": "LOW",
                    "value": data.value,
                    "threshold": sensor["alert_min"]
                })
                # Create alert record
                cur.execute("""
                    INSERT INTO industrial_alerts (sensor_id, alert_type, alert_value, threshold)
                    VALUES (%s, 'LOW', %s, %s)
                """, (data.sensor_id, data.value, sensor["alert_min"]))
                
            if sensor["alert_max"] and data.value > sensor["alert_max"]:
                alerts.append({
                    "type": "HIGH",
                    "value": data.value,
                    "threshold": sensor["alert_max"]
                })
                cur.execute("""
                    INSERT INTO industrial_alerts (sensor_id, alert_type, alert_value, threshold)
                    VALUES (%s, 'HIGH', %s, %s)
                """, (data.sensor_id, data.value, sensor["alert_max"]))
        
        conn.commit()
        
        return {
            "status": "ok",
            "alerts": alerts,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close()
        conn.close()


@app.get("/api/industrial/alerts", tags=["Industrial"])
def get_industrial_alerts(acknowledged: bool = False):
    """Get industrial alerts"""
    cache_key = f"industrial_alerts_{acknowledged}"
    
    def _get_alerts():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT a.*, s.sensor_name, s.location, s.machine_id
            FROM industrial_alerts a
            JOIN industrial_sensors s ON a.sensor_id = s.id
            WHERE a.acknowledged = %s
            ORDER BY a.created_at DESC
        """, (acknowledged,))
        alerts = cur.fetchall()
        cur.close()
        conn.close()
        return alerts
    
    return get_cached_or_set(cache_key, _get_alerts)


@app.post("/api/industrial/alerts/{alert_id}/acknowledge", tags=["Industrial"])
def acknowledge_alert(alert_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE industrial_alerts SET acknowledged=1 WHERE id=%s", (alert_id,))
    conn.commit()
    cur.close()
    conn.close()
    
    cache.pop("industrial_alerts_False", None)
    cache.pop("industrial_alerts_True", None)
    
    return {"message": "Alert acknowledged"}


# ══════════════════════════════════════════════════════════════
#  SMART CITY ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.get("/api/city/street-lights", tags=["Smart City"])
def get_street_lights():
    """Get all street lights status"""
    cache_key = "street_lights"
    
    def _get_lights():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM street_lights ORDER BY location_name, light_id")
        lights = cur.fetchall()
        cur.close()
        conn.close()
        return lights
    
    return get_cached_or_set(cache_key, _get_lights)


@app.post("/api/city/street-lights/control", tags=["Smart City"])
def control_street_light(control: StreetLightControl):
    """Control individual street light"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        UPDATE street_lights 
        SET is_on=%s, intensity=%s, auto_mode=%s, last_updated=NOW()
        WHERE light_id=%s
    """, (control.state, control.intensity, control.auto_mode, control.light_id))
    
    affected = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    
    if affected == 0:
        raise HTTPException(404, "Street light not found")
    
    cache.pop("street_lights", None)
    
    return {"message": f"Street light {control.light_id} set to {'ON' if control.state else 'OFF'}"}


@app.post("/api/city/street-lights/bulk-control", tags=["Smart City"])
def bulk_control_street_lights(controls: List[StreetLightControl]):
    """Control multiple street lights at once"""
    conn = get_db()
    cur = conn.cursor()
    
    updated = 0
    for control in controls:
        cur.execute("""
            UPDATE street_lights 
            SET is_on=%s, intensity=%s, auto_mode=%s, last_updated=NOW()
            WHERE light_id=%s
        """, (control.state, control.intensity, control.auto_mode, control.light_id))
        updated += cur.rowcount
    
    conn.commit()
    cur.close()
    conn.close()
    
    cache.pop("street_lights", None)
    
    return {"message": f"Updated {updated} street lights"}


@app.get("/api/city/smart-bins", tags=["Smart City"])
def get_smart_bins(alert_only: bool = False):
    """Get all smart bins status"""
    cache_key = f"smart_bins_{alert_only}"
    
    def _get_bins():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        
        if alert_only:
            cur.execute("""
                SELECT * FROM smart_bins 
                WHERE fill_level >= alert_threshold
                ORDER BY fill_level DESC
            """)
        else:
            cur.execute("SELECT * FROM smart_bins ORDER BY fill_level DESC")
            
        bins = cur.fetchall()
        cur.close()
        conn.close()
        return bins
    
    return get_cached_or_set(cache_key, _get_bins)


@app.post("/api/city/smart-bins/update", tags=["Smart City"])
def update_smart_bin(data: SmartBinData):
    """Update smart bin data from sensor"""
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    
    try:
        cur.execute("""
            UPDATE smart_bins 
            SET fill_level=%s, temperature=%s, battery_level=%s, last_updated=NOW()
            WHERE bin_id=%s
        """, (data.fill_level, data.temperature, data.battery, data.bin_id))
        
        affected = cur.rowcount
        
        # Check if bin needs collection
        alert = None
        if affected > 0:
            cur.execute("SELECT alert_threshold FROM smart_bins WHERE bin_id=%s", (data.bin_id,))
            threshold = cur.fetchone()["alert_threshold"]
            
            if data.fill_level >= threshold:
                alert = {
                    "bin_id": data.bin_id,
                    "fill_level": data.fill_level,
                    "message": "Bin needs emptying!"
                }
        
        conn.commit()
        
        return {
            "status": "ok",
            "updated": affected > 0,
            "alert": alert
        }
        
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close()
        conn.close()


@app.post("/api/city/smart-bins/{bin_id}/schedule", tags=["Smart City"])
def schedule_bin_collection(bin_id: str, schedule: BinCollectionSchedule):
    """Schedule bin collection"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO bin_collection_schedule (bin_id, scheduled_date, scheduled_time)
        VALUES (%s, %s, %s)
    """, (bin_id, schedule.scheduled_date, schedule.scheduled_time))
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {"message": f"Collection scheduled for bin {bin_id}"}


# ══════════════════════════════════════════════════════════════
#  OFFICE AUTOMATION ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.get("/api/office/zones", tags=["Office"])
def get_office_zones():
    """Get all office zones"""
    cache_key = "office_zones"
    
    def _get_zones():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT oz.*, r.name as room_name, r.icon as room_icon
            FROM office_zones oz
            LEFT JOIN rooms r ON oz.room_id = r.id
            ORDER BY oz.floor, oz.zone_name
        """)
        zones = cur.fetchall()
        cur.close()
        conn.close()
        return zones
    
    return get_cached_or_set(cache_key, _get_zones)


@app.post("/api/office/zones", status_code=201, tags=["Office"])
def create_office_zone(zone: OfficeZoneCreate):
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO office_zones (zone_name, floor, department, room_id)
        VALUES (%s, %s, %s, %s)
    """, (zone.zone_name, zone.floor, zone.department, zone.room_id))
    
    zone_id = cur.lastrowid
    conn.commit()
    cur.close()
    conn.close()
    
    cache.pop("office_zones", None)
    
    return {"id": zone_id, "message": "Office zone created"}


@app.get("/api/office/lighting", tags=["Office"])
def get_office_lighting(zone_id: Optional[int] = None):
    """Get office lighting status"""
    cache_key = f"office_lighting_{zone_id}" if zone_id else "office_lighting"
    
    def _get_lighting():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        
        if zone_id:
            cur.execute("""
                SELECT ol.*, oz.zone_name
                FROM office_lighting ol
                JOIN office_zones oz ON ol.zone_id = oz.id
                WHERE ol.zone_id = %s
                ORDER BY ol.light_id
            """, (zone_id,))
        else:
            cur.execute("""
                SELECT ol.*, oz.zone_name
                FROM office_lighting ol
                JOIN office_zones oz ON ol.zone_id = oz.id
                ORDER BY oz.zone_name, ol.light_id
            """)
            
        lights = cur.fetchall()
        cur.close()
        conn.close()
        return lights
    
    return get_cached_or_set(cache_key, _get_lighting)


@app.post("/api/office/lighting/control", tags=["Office"])
def control_office_lighting(control: OfficeLightingControl):
    """Control office lighting"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        UPDATE office_lighting 
        SET is_on=%s, brightness=%s, auto_mode=%s, last_updated=NOW()
        WHERE zone_id=%s AND light_id=%s
    """, (control.state, control.brightness, control.auto_mode, control.zone_id, control.light_id))
    
    affected = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    
    if affected == 0:
        raise HTTPException(404, "Light not found")
    
    cache.pop(f"office_lighting_{control.zone_id}", None)
    cache.pop("office_lighting", None)
    
    return {"message": f"Light {control.light_id} in zone {control.zone_id} controlled"}


@app.get("/api/office/ventilation", tags=["Office"])
def get_ventilation_status(zone_id: Optional[int] = None):
    """Get ventilation system status"""
    cache_key = f"ventilation_{zone_id}" if zone_id else "ventilation"
    
    def _get_ventilation():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        
        if zone_id:
            cur.execute("""
                SELECT v.*, oz.zone_name
                FROM ventilation_systems v
                JOIN office_zones oz ON v.zone_id = oz.id
                WHERE v.zone_id = %s
            """, (zone_id,))
        else:
            cur.execute("""
                SELECT v.*, oz.zone_name
                FROM ventilation_systems v
                JOIN office_zones oz ON v.zone_id = oz.id
                ORDER BY oz.zone_name
            """)
            
        systems = cur.fetchall()
        cur.close()
        conn.close()
        return systems
    
    return get_cached_or_set(cache_key, _get_ventilation)


@app.post("/api/office/ventilation/control", tags=["Office"])
def control_ventilation(control: VentilationControl):
    """Control ventilation system"""
    conn = get_db()
    cur = conn.cursor()
    
    is_running = control.fan_speed > 0
    
    cur.execute("""
        UPDATE ventilation_systems 
        SET fan_speed=%s, is_running=%s, auto_mode=%s, last_updated=NOW()
        WHERE zone_id=%s
    """, (control.fan_speed, is_running, control.auto_mode, control.zone_id))
    
    affected = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    
    if affected == 0:
        raise HTTPException(404, "Ventilation system not found")
    
    cache.pop(f"ventilation_{control.zone_id}", None)
    cache.pop("ventilation", None)
    
    return {"message": f"Ventilation in zone {control.zone_id} set to speed {control.fan_speed}"}


@app.post("/api/office/occupancy", tags=["Office"])
def update_occupancy(data: dict):
    """Update occupancy detection (from PIR sensors)"""
    zone_id = data.get("zone_id")
    occupied = data.get("occupied", False)
    
    if not zone_id:
        raise HTTPException(400, "zone_id required")
    
    conn = get_db()
    cur = conn.cursor()
    
    # Update lighting occupancy
    cur.execute("""
        UPDATE office_lighting 
        SET occupancy_detected=%s
        WHERE zone_id=%s
    """, (occupied, zone_id))
    
    # Auto-control lights based on occupancy if in auto mode
    if occupied:
        cur.execute("""
            UPDATE office_lighting 
            SET is_on=1 
            WHERE zone_id=%s AND auto_mode=1
        """, (zone_id,))
    
    conn.commit()
    cur.close()
    conn.close()
    
    cache.pop(f"office_lighting_{zone_id}", None)
    
    return {"status": "ok", "zone_id": zone_id, "occupied": occupied}


# ══════════════════════════════════════════════════════════════
#  ACCESS CONTROL ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.get("/api/office/access/users", tags=["Access Control"])
def get_access_users():
    """Get all authorized users"""
    cache_key = "access_users"
    
    def _get_users():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id, user_id, user_name, access_level, has_access, last_access FROM access_control")
        users = cur.fetchall()
        cur.close()
        conn.close()
        return users
    
    return get_cached_or_set(cache_key, _get_users)


@app.post("/api/office/access/grant", tags=["Access Control"])
def grant_access(user: AccessGrant):
    """Grant access to a user"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO access_control (user_id, user_name, access_level, has_access, card_rfid)
        VALUES (%s, %s, %s, 1, %s)
        ON DUPLICATE KEY UPDATE
        user_name=%s, access_level=%s, has_access=1, card_rfid=%s
    """, (user.user_id, user.user_name, user.access_level, user.card_rfid,
          user.user_name, user.access_level, user.card_rfid))
    
    conn.commit()
    cur.close()
    conn.close()
    
    cache.pop("access_users", None)
    
    return {"message": f"Access granted to {user.user_name}"}


@app.post("/api/office/access/revoke/{user_id}", tags=["Access Control"])
def revoke_access(user_id: str):
    """Revoke access from a user"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("UPDATE access_control SET has_access=0 WHERE user_id=%s", (user_id,))
    affected = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    
    if affected == 0:
        raise HTTPException(404, "User not found")
    
    cache.pop("access_users", None)
    
    return {"message": f"Access revoked for user {user_id}"}


@app.post("/api/office/access/verify", tags=["Access Control"])
def verify_access(request: AccessRequest):
    """Verify access attempt (for facial recognition/RFID)"""
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    
    try:
        # Check if user exists and has access
        cur.execute("""
            SELECT * FROM access_control 
            WHERE user_id=%s AND has_access=1
        """, (request.user_id,))
        
        user = cur.fetchone()
        
        if user:
            # Log successful access
            cur.execute("""
                INSERT INTO access_logs (user_id, access_point, granted, image_capture)
                VALUES (%s, %s, 1, %s)
            """, (request.user_id, request.access_point, request.image))
            
            # Update last access
            cur.execute("""
                UPDATE access_control SET last_access=NOW() 
                WHERE user_id=%s
            """, (request.user_id,))
            
            conn.commit()
            
            return {
                "granted": True,
                "user_name": user["user_name"],
                "access_level": user["access_level"],
                "message": "Access granted"
            }
        else:
            # Log denied access
            cur.execute("""
                INSERT INTO access_logs (user_id, access_point, granted, image_capture)
                VALUES (%s, %s, 0, %s)
            """, (request.user_id or "unknown", request.access_point, request.image))
            
            conn.commit()
            
            return {
                "granted": False,
                "message": "Access denied"
            }
            
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close()
        conn.close()


@app.get("/api/office/access/logs", tags=["Access Control"])
def get_access_logs(limit: int = 100):
    """Get recent access logs"""
    cache_key = f"access_logs_{limit}"
    
    def _get_logs():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT * FROM access_logs 
            ORDER BY timestamp DESC 
            LIMIT %s
        """, (limit,))
        logs = cur.fetchall()
        cur.close()
        conn.close()
        return logs
    
    return get_cached_or_set(cache_key, _get_logs)


# ══════════════════════════════════════════════════════════════
#  ENERGY MANAGEMENT ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.get("/api/energy/meters", tags=["Energy"])
def get_energy_meters():
    """Get all energy meters"""
    cache_key = "energy_meters"
    
    def _get_meters():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM energy_meters ORDER BY location, meter_id")
        meters = cur.fetchall()
        cur.close()
        conn.close()
        return meters
    
    return get_cached_or_set(cache_key, _get_meters)


@app.post("/api/energy/reading", tags=["Energy"])
def record_energy_reading(reading: EnergyReading):
    """Record energy consumption reading"""
    conn = get_db()
    cur = conn.cursor()
    
    try:
        # Update current reading
        cur.execute("""
            UPDATE energy_meters 
            SET current_power=%s, last_reading=NOW()
            WHERE meter_id=%s
        """, (reading.power, reading.meter_id))
        
        # Insert historical reading
        cur.execute("""
            INSERT INTO energy_readings (meter_id, power, energy_kwh)
            VALUES (%s, %s, %s)
        """, (reading.meter_id, reading.power, reading.energy_kwh))
        
        conn.commit()
        
        return {"status": "ok", "meter_id": reading.meter_id}
        
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close()
        conn.close()


@app.get("/api/energy/consumption", tags=["Energy"])
def get_energy_consumption(meter_id: Optional[str] = None, hours: int = 24):
    """Get energy consumption history"""
    cache_key = f"energy_consumption_{meter_id}_{hours}"
    
    def _get_consumption():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        
        since = datetime.now() - timedelta(hours=hours)
        
        if meter_id:
            cur.execute("""
                SELECT * FROM energy_readings 
                WHERE meter_id=%s AND reading_time > %s
                ORDER BY reading_time DESC
            """, (meter_id, since))
        else:
            cur.execute("""
                SELECT * FROM energy_readings 
                WHERE reading_time > %s
                ORDER BY reading_time DESC
            """, (since,))
            
        readings = cur.fetchall()
        cur.close()
        conn.close()
        return readings
    
    return get_cached_or_set(cache_key, _get_consumption)


# ══════════════════════════════════════════════════════════════
#  WEATHER STATION ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.post("/api/weather/update", tags=["Weather"])
def update_weather_data(data: WeatherData):
    """Update weather station data"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO weather_data 
        (temperature, humidity, pressure, wind_speed, wind_direction, rainfall, light_level)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (data.temperature, data.humidity, data.pressure, data.wind_speed,
          data.wind_direction, data.rainfall, data.light_level))
    
    conn.commit()
    cur.close()
    conn.close()
    
    cache.pop("weather_current", None)
    cache.pop("weather_history", None)
    
    return {"status": "ok"}


@app.get("/api/weather/current", tags=["Weather"])
def get_current_weather():
    """Get current weather conditions"""
    cache_key = "weather_current"
    
    def _get_weather():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM weather_data ORDER BY recorded_at DESC LIMIT 1")
        weather = cur.fetchone()
        cur.close()
        conn.close()
        return weather or {}
    
    return get_cached_or_set(cache_key, _get_weather)


@app.get("/api/weather/history", tags=["Weather"])
def get_weather_history(hours: int = 24):
    """Get weather history"""
    cache_key = f"weather_history_{hours}"
    
    def _get_history():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        since = datetime.now() - timedelta(hours=hours)
        cur.execute("""
            SELECT * FROM weather_data 
            WHERE recorded_at > %s
            ORDER BY recorded_at DESC
        """, (since,))
        history = cur.fetchall()
        cur.close()
        conn.close()
        return history
    
    return get_cached_or_set(cache_key, _get_history)


# ══════════════════════════════════════════════════════════════
#  DASHBOARD - UNIFIED VIEW
# ══════════════════════════════════════════════════════════════

@app.get("/api/dashboard", tags=["Dashboard"])
def get_unified_dashboard():
    """Get unified dashboard data for all systems"""
    cache_key = "unified_dashboard"
    
    def _get_dashboard():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        
        # Rooms summary
        cur.execute("""
            SELECT r.room_type, COUNT(*) as count
            FROM rooms r
            GROUP BY r.room_type
        """)
        rooms_by_type = cur.fetchall()
        
        # Devices summary
        cur.execute("""
            SELECT 
                COUNT(*) as total_devices,
                SUM(CASE WHEN is_sensor=1 THEN 1 ELSE 0 END) as total_sensors,
                SUM(CASE WHEN current_state='ON' THEN 1 ELSE 0 END) as active_devices
            FROM devices
        """)
        device_stats = cur.fetchone()
        
        # Devices by type
        cur.execute("""
            SELECT type, COUNT(*) as count
            FROM devices
            GROUP BY type
            ORDER BY count DESC
            LIMIT 10
        """)
        top_device_types = cur.fetchall()
        
        # Recent sensor readings
        cur.execute("""
            SELECT id, name, type, sensor_value, last_seen
            FROM devices
            WHERE is_sensor=1 AND last_seen IS NOT NULL
            ORDER BY last_seen DESC
            LIMIT 20
        """)
        recent_sensors = cur.fetchall()
        
        # Active scenes
        cur.execute("SELECT COUNT(*) as active_scenes FROM scenes WHERE is_enabled=1")
        active_scenes = cur.fetchone()
        
        # Automation rules
        cur.execute("SELECT COUNT(*) as total_rules, SUM(CASE WHEN enabled=1 THEN 1 ELSE 0 END) as enabled_rules FROM automation_rules")
        rule_stats = cur.fetchone()
        
        # Agriculture summary
        cur.execute("SELECT COUNT(*) as agri_zones FROM agricultural_zones")
        agri_zones = cur.fetchone()
        
        # Industrial alerts
        cur.execute("SELECT COUNT(*) as active_alerts FROM industrial_alerts WHERE acknowledged=0")
        active_alerts = cur.fetchone()
        
        # Smart city
        cur.execute("SELECT COUNT(*) as bins_needing_attention FROM smart_bins WHERE fill_level >= alert_threshold")
        bins_alert = cur.fetchone()
        
        cur.execute("SELECT COUNT(*) as lights_on FROM street_lights WHERE is_on=1")
        lights_on = cur.fetchone()
        
        # Office
        cur.execute("SELECT COUNT(*) as occupied_zones FROM office_lighting WHERE occupancy_detected=1")
        occupied_zones = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return {
            "summary": {
                "rooms_by_type": rooms_by_type,
                "total_devices": device_stats["total_devices"] if device_stats else 0,
                "total_sensors": device_stats["total_sensors"] if device_stats else 0,
                "active_devices": device_stats["active_devices"] if device_stats else 0,
                "active_scenes": active_scenes["active_scenes"] if active_scenes else 0,
                "total_rules": rule_stats["total_rules"] if rule_stats else 0,
                "enabled_rules": rule_stats["enabled_rules"] if rule_stats else 0,
            },
            "devices": {
                "top_types": top_device_types,
                "recent_sensors": recent_sensors
            },
            "agriculture": {
                "zones": agri_zones["agri_zones"] if agri_zones else 0
            },
            "industrial": {
                "active_alerts": active_alerts["active_alerts"] if active_alerts else 0
            },
            "smart_city": {
                "bins_needing_attention": bins_alert["bins_needing_attention"] if bins_alert else 0,
                "lights_on": lights_on["lights_on"] if lights_on else 0
            },
            "office": {
                "occupied_zones": occupied_zones["occupied_zones"] if occupied_zones else 0
            },
            "timestamp": datetime.now().isoformat()
        }
    
    return get_cached_or_set(cache_key, _get_dashboard)


# ══════════════════════════════════════════════════════════════
#  ESP32 ENDPOINTS (Optimized for speed)
# ══════════════════════════════════════════════════════════════

@app.post("/api/esp32/heartbeat", tags=["ESP32"])
def esp32_heartbeat(data: ESP32Heartbeat):
    """ESP32 heartbeat - updates last_seen timestamp"""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE devices SET last_seen=NOW() WHERE id=%s", (data.device_id,))
        conn.commit()
        
        # Log occasionally
        if random.randint(1, 20) == 1:
            log.info(f"[ESP32] Heartbeat device {data.device_id}")
            
        return {"status": "ok", "t": int(time.time())}
    finally:
        cur.close()
        conn.close()


@app.post("/api/esp32/sensor", tags=["ESP32"])
def esp32_sensor_data(data: ESP32SensorData):
    """Structured sensor endpoint — optimized fast path"""
    conn = get_db()
    cur = conn.cursor()
    try:
        value_str = f"{data.value}{data.unit}"
        cur.execute(
            "UPDATE devices SET sensor_value=%s, last_seen=NOW() WHERE id=%s",
            (value_str, data.device_id),
        )
        
        # Log to history occasionally
        if random.randint(1, 10) == 1:
            cur.execute(
                "INSERT INTO device_history (device_id, event_type, event_data) VALUES (%s,'sensor',%s)",
                (data.device_id, json.dumps({"value": data.value, "unit": data.unit, "type": data.sensor_type})),
            )
        
        conn.commit()
        return {"status": "ok"}
    finally:
        cur.close()
        conn.close()


@app.get("/api/esp32/command/{device_id}", tags=["ESP32"])
def esp32_get_command(device_id: int):
    """ESP32 polls this to get the latest commanded state - ultra fast"""
    cache_key = f"esp32_cmd_{device_id}"
    
    def _get_command():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT current_state, type, name FROM devices WHERE id=%s", (device_id,))
        device = cur.fetchone()
        cur.close()
        conn.close()
        
        if not device:
            raise HTTPException(404, "Device not found")
        
        return {
            "device_id": device_id,
            "command": device["current_state"],
            "type": device["type"],
            "name": device["name"],
            "t": int(time.time()),
        }
    
    try:
        return get_cached_or_set(cache_key, _get_command)
    except HTTPException:
        raise


@app.get("/api/esp32/commands/room/{room_id}", tags=["ESP32"])
def esp32_get_room_commands(room_id: int):
    """Get all device commands for a room (bulk polling)"""
    cache_key = f"esp32_room_cmds_{room_id}"
    
    def _get_commands():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT id, current_state, type, name 
            FROM devices 
            WHERE room_id=%s AND is_sensor=0
        """, (room_id,))
        devices = cur.fetchall()
        cur.close()
        conn.close()
        
        return {
            "room_id": room_id,
            "devices": devices,
            "t": int(time.time())
        }
    
    return get_cached_or_set(cache_key, _get_commands)


@app.post("/api/esp32/register", tags=["ESP32"])
def esp32_register(body: ESP32RegisterBody):
    """Register ESP32 device with MAC address"""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE devices SET esp32_mac=%s, last_seen=NOW() WHERE id=%s",
            (body.mac_address, body.device_id),
        )
        affected = cur.rowcount
        conn.commit()
        
        if affected == 0:
            raise HTTPException(404, "Device not found")
        
        return {
            "status": "registered", 
            "device_id": body.device_id,
            "t": int(time.time())
        }
    finally:
        cur.close()
        conn.close()


# ══════════════════════════════════════════════════════════════
#  AUTOMATION (Optimized)
# ══════════════════════════════════════════════════════════════

def _check_automation_fast(cur, conn, sensor_device_id: int, raw_value: str):
    """Optimized automation check - only runs if needed"""
    try:
        # Quick check if any rules exist for this sensor
        cur.execute(
            "SELECT COUNT(*) FROM automation_rules WHERE enabled=TRUE AND trigger_type='sensor' "
            "AND JSON_EXTRACT(trigger_config, '$.sensor_id') = %s",
            (sensor_device_id,)
        )
        count = cur.fetchone()[0]
        if count == 0:
            return  # No rules for this sensor

        # Extract numeric value
        nums = re.findall(r"[-+]?\d*\.?\d+", raw_value)
        value = float(nums[0]) if nums else None
        if value is None:
            return

        # Get all relevant rules
        cur.execute(
            "SELECT * FROM automation_rules WHERE enabled=TRUE AND trigger_type='sensor'"
        )
        rules = cur.fetchall()
        
        for rule in rules:
            cfg = json.loads(rule[5]) if isinstance(rule[5], str) else rule[5]  # trigger_config
            if cfg.get("sensor_id") != sensor_device_id:
                continue
                
            threshold = float(cfg.get("threshold", 0))
            cond = cfg.get("condition", ">")
            
            triggered = (
                (cond == ">" and value > threshold) or
                (cond == "<" and value < threshold) or
                (cond == ">=" and value >= threshold) or
                (cond == "<=" and value <= threshold) or
                (cond == "==" and value == threshold)
            )
            
            if triggered:
                cur.execute(
                    "UPDATE devices SET current_state=%s WHERE id=%s",
                    (rule[8], rule[7]),  # action_command, action_device_id
                )
                cur.execute(
                    "UPDATE automation_rules SET last_triggered=NOW() WHERE id=%s",
                    (rule[0],),
                )
                conn.commit()
                log.info(f"[Auto] Rule '{rule[1]}' triggered")
    except Exception as e:
        log.warning(f"[Auto] check failed: {e}")


@app.get("/api/automation/rules", tags=["Automation"])
def get_automation_rules(trigger_type: Optional[str] = None):
    """Get all automation rules"""
    cache_key = f"automation_rules_{trigger_type}" if trigger_type else "automation_rules"
    
    def _get_rules():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        
        if trigger_type:
            cur.execute("SELECT * FROM automation_rules WHERE trigger_type=%s ORDER BY id", (trigger_type,))
        else:
            cur.execute("SELECT * FROM automation_rules ORDER BY id")
            
        rules = cur.fetchall()
        cur.close()
        conn.close()
        return rules
    
    return get_cached_or_set(cache_key, _get_rules)


@app.post("/api/automation/rules", tags=["Automation"])
def create_automation_rule(rule: AutomationRuleCreate):
    """Create a new automation rule"""
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            INSERT INTO automation_rules
            (name, enabled, trigger_type, trigger_config, condition_type,
             condition_config, action_device_id, action_command, action_parameters)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            rule.name, rule.enabled, rule.trigger_type,
            json.dumps(rule.trigger_config), rule.condition_type,
            json.dumps(rule.condition_config) if rule.condition_config else None,
            rule.action_device_id, rule.action_command,
            json.dumps(rule.action_parameters or {}),
        ))
        
        rule_id = cur.lastrowid
        conn.commit()
        
        cur.execute("SELECT * FROM automation_rules WHERE id=%s", (rule_id,))
        new_rule = cur.fetchone()
        
        cache.pop("automation_rules", None)
        cache.pop("automation_rules_sensor", None)
        cache.pop("automation_rules_time", None)
        
        return new_rule
    finally:
        cur.close()
        conn.close()


@app.put("/api/automation/rules/{rule_id}/toggle", tags=["Automation"])
def toggle_automation_rule(rule_id: int):
    """Enable or disable an automation rule"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("SELECT enabled FROM automation_rules WHERE id=%s", (rule_id,))
    result = cur.fetchone()
    if not result:
        cur.close()
        conn.close()
        raise HTTPException(404, "Rule not found")
    
    new_state = not result[0]
    cur.execute("UPDATE automation_rules SET enabled=%s WHERE id=%s", (new_state, rule_id))
    conn.commit()
    cur.close()
    conn.close()
    
    cache.pop("automation_rules", None)
    cache.pop("automation_rules_sensor", None)
    cache.pop("automation_rules_time", None)
    
    return {"id": rule_id, "enabled": new_state}


@app.delete("/api/automation/rules/{rule_id}", tags=["Automation"])
def delete_automation_rule(rule_id: int):
    """Delete an automation rule"""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM automation_rules WHERE id=%s", (rule_id,))
    affected = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    
    if affected == 0:
        raise HTTPException(404, "Rule not found")
    
    cache.pop("automation_rules", None)
    cache.pop("automation_rules_sensor", None)
    cache.pop("automation_rules_time", None)
    
    return {"success": True, "message": "Rule deleted"}


# ══════════════════════════════════════════════════════════════
#  HISTORY ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.get("/api/devices/{device_id}/history", tags=["History"])
def get_device_history(device_id: int, hours: int = 24, event_type: Optional[str] = None):
    """Get device history"""
    cache_key = f"history_{device_id}_{hours}_{event_type}"
    
    def _get_history():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        since = datetime.now() - timedelta(hours=hours)
        
        if event_type:
            cur.execute(
                "SELECT * FROM device_history WHERE device_id=%s AND created_at>%s AND event_type=%s ORDER BY created_at DESC",
                (device_id, since, event_type),
            )
        else:
            cur.execute(
                "SELECT * FROM device_history WHERE device_id=%s AND created_at>%s ORDER BY created_at DESC",
                (device_id, since),
            )
            
        history = cur.fetchall()
        cur.close()
        conn.close()
        return history
    
    return get_cached_or_set(cache_key, _get_history)


@app.get("/api/history/recent", tags=["History"])
def get_recent_history(limit: int = 50):
    """Get recent events across all devices"""
    cache_key = f"recent_history_{limit}"
    
    def _get_history():
        conn = get_db()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT h.*, d.name as device_name, d.type, d.room_id
            FROM device_history h
            JOIN devices d ON h.device_id = d.id
            ORDER BY h.created_at DESC
            LIMIT %s
        """, (limit,))
        history = cur.fetchall()
        cur.close()
        conn.close()
        return history
    
    return get_cached_or_set(cache_key, _get_history)


# ══════════════════════════════════════════════════════════════
#  SYSTEM STATUS & HEALTH
# ══════════════════════════════════════════════════════════════

@app.get("/", tags=["System"])
@app.get("/health", tags=["System"])
def root():
    """Ultra-fast health check endpoint"""
    return {
        "status": "ok",
        "system": "UNIFIED IoT CONTROL SYSTEM",
        "version": "5.0.0-UNIFIED",
        "modules": ["home", "agriculture", "industrial", "smart_city", "office"],
        "t": int(time.time()),
    }


@app.get("/api/system/status", tags=["System"])
def get_system_status():
    """Get complete system status"""
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    
    # Database stats
    cur.execute("SELECT COUNT(*) as total FROM rooms")
    rooms_count = cur.fetchone()
    
    cur.execute("SELECT COUNT(*) as total FROM devices")
    devices_count = cur.fetchone()
    
    cur.execute("SELECT COUNT(*) as total FROM devices WHERE is_sensor=1")
    sensors_count = cur.fetchone()
    
    cur.execute("SELECT COUNT(*) as total FROM scenes")
    scenes_count = cur.fetchone()
    
    cur.execute("SELECT COUNT(*) as total FROM automation_rules WHERE enabled=1")
    active_rules = cur.fetchone()
    
    # Online devices (last seen in last 5 minutes)
    five_min_ago = datetime.now() - timedelta(minutes=5)
    cur.execute("SELECT COUNT(*) as online FROM devices WHERE last_seen > %s", (five_min_ago,))
    online_devices = cur.fetchone()
    
    cur.close()
    conn.close()
    
    return {
        "database": {
            "rooms": rooms_count["total"] if rooms_count else 0,
            "devices": devices_count["total"] if devices_count else 0,
            "sensors": sensors_count["total"] if sensors_count else 0,
            "scenes": scenes_count["total"] if scenes_count else 0,
            "active_rules": active_rules["total"] if active_rules else 0,
        },
        "network": {
            "online_devices": online_devices["online"] if online_devices else 0,
            "offline_devices": (devices_count["total"] if devices_count else 0) - (online_devices["online"] if online_devices else 0)
        },
        "cache": {
            "size": len(cache),
            "max_size": CACHE_MAX_SIZE,
            "ttl_seconds": CACHE_TTL
        },
        "uptime": int(time.time() - start_time) if 'start_time' in globals() else 0,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/api/system/cache/stats", tags=["System"])
def get_cache_stats():
    """Get cache performance stats"""
    return {
        "cache_size": len(cache),
        "cache_max": CACHE_MAX_SIZE,
        "cache_ttl": CACHE_TTL,
        "uptime": int(time.time() - start_time) if 'start_time' in globals() else 0,
    }


@app.post("/api/system/cache/clear", tags=["System"])
def clear_cache():
    """Clear all cached data"""
    cache.clear()
    return {"message": "Cache cleared", "size": 0}


# ══════════════════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════════════════

# Track start time
start_time = time.time()

# Print startup banner
print("\n" + "="*70)
print("  UNIFIED IoT CONTROL SYSTEM v5.0.0")
print("="*70)
print("  Modules Loaded:")
print("    ✓ Smart Home Automation")
print("    ✓ Agriculture & Livestock")
print("    ✓ Industrial IoT Monitoring")
print("    ✓ Smart City Infrastructure")
print("    ✓ Office Automation")
print("    ✓ Energy Management")
print("    ✓ Weather Station")
print("-"*70)
print("  Optimizations:")
print("    • Database connection pooling")
print("    • Response compression (GZip)")
print("    • Response caching (5s TTL)")
print("    • Function result caching (lru_cache)")
print("    • Optimized database indexes")
print("    • Reduced logging in production")
print("-"*70)
print("  Endpoints:")
print("    • http://localhost:8000/")
print("    • http://localhost:8000/docs")
print("    • http://localhost:8000/redoc")
print("="*70 + "\n")

log.info("=" * 60)
log.info("  UNIFIED IOT CONTROL SYSTEM v5.0.0")
log.info("  http://localhost:8000  |  /docs for Swagger UI")
log.info("  ALL MODULES ACTIVE:")
log.info("    • Smart Home")
log.info("    • Agriculture/Livestock")
log.info("    • Industrial IoT")
log.info("    • Smart City")
log.info("    • Office Automation")
log.info("=" * 60)
