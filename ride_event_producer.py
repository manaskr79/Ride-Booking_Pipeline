"""
Ride Booking Kafka Producer
Simulates real-time ride booking events (5K+ events/day)
"""

import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_RIDE_REQUESTS     = 'ride-requests'
TOPIC_RIDE_COMPLETED    = 'ride-completed'
TOPIC_DRIVER_LOCATION   = 'driver-locations'
EVENTS_PER_DAY          = 5000
SLEEP_INTERVAL          = 86400 / EVENTS_PER_DAY   # ~17.28 seconds per event

# --- Geo Zones (Jaipur / Generic City) ---
CITY_ZONES = [
    {"zone_id": "Z001", "name": "City Center",       "lat_range": (26.88, 26.93), "lng_range": (75.78, 75.83)},
    {"zone_id": "Z002", "name": "Airport",           "lat_range": (26.82, 26.84), "lng_range": (75.80, 75.82)},
    {"zone_id": "Z003", "name": "Tech Park",         "lat_range": (26.85, 26.90), "lng_range": (75.75, 75.80)},
    {"zone_id": "Z004", "name": "Old City",          "lat_range": (26.92, 26.96), "lng_range": (75.82, 75.88)},
    {"zone_id": "Z005", "name": "Residential North", "lat_range": (26.95, 27.00), "lng_range": (75.76, 75.82)},
    {"zone_id": "Z006", "name": "Mall District",     "lat_range": (26.87, 26.91), "lng_range": (75.84, 75.90)},
]

RIDE_TYPES    = ["Economy", "Premium", "Shared", "Auto", "Bike"]
PAYMENT_MODES = ["Cash", "Card", "Wallet", "UPI"]
CANCELLATION_REASONS = [
    "Driver too far", "Changed plans", "Found alternative",
    "Price too high", "Long wait time"
]


def get_random_coords(zone):
    lat = random.uniform(*zone["lat_range"])
    lng = random.uniform(*zone["lng_range"])
    return round(lat, 6), round(lng, 6)


def calculate_surge_multiplier(hour: int, zone_id: str) -> float:
    """Surge pricing logic based on time and zone demand."""
    base = 1.0
    # Morning rush
    if 7 <= hour <= 9:
        base += 0.5
    # Evening rush
    elif 17 <= hour <= 20:
        base += 0.7
    # Late night premium
    elif 23 <= hour or hour <= 4:
        base += 0.3
    # Airport zone always higher
    if zone_id == "Z002":
        base += 0.2
    return round(min(base + random.uniform(-0.1, 0.1), 3.0), 2)


def generate_ride_request_event() -> dict:
    pickup_zone  = random.choice(CITY_ZONES)
    dropoff_zone = random.choice([z for z in CITY_ZONES if z["zone_id"] != pickup_zone["zone_id"]])
    pickup_lat, pickup_lng   = get_random_coords(pickup_zone)
    dropoff_lat, dropoff_lng = get_random_coords(dropoff_zone)
    now  = datetime.utcnow()
    hour = now.hour

    ride_type       = random.choice(RIDE_TYPES)
    base_fare_map   = {"Economy": 50, "Premium": 120, "Shared": 30, "Auto": 40, "Bike": 25}
    distance_km     = round(random.uniform(2.0, 25.0), 2)
    surge           = calculate_surge_multiplier(hour, pickup_zone["zone_id"])
    estimated_fare  = round(base_fare_map[ride_type] + distance_km * 12 * surge, 2)

    return {
        "event_id":           str(uuid.uuid4()),
        "event_type":         "RIDE_REQUEST",
        "timestamp":          now.isoformat(),
        "ride_id":            f"RD-{uuid.uuid4().hex[:10].upper()}",
        "user_id":            f"USR-{random.randint(10000, 99999)}",
        "driver_id":          None,
        "ride_type":          ride_type,
        "status":             "REQUESTED",
        "pickup": {
            "zone_id":    pickup_zone["zone_id"],
            "zone_name":  pickup_zone["name"],
            "latitude":   pickup_lat,
            "longitude":  pickup_lng,
        },
        "dropoff": {
            "zone_id":    dropoff_zone["zone_id"],
            "zone_name":  dropoff_zone["name"],
            "latitude":   dropoff_lat,
            "longitude":  dropoff_lng,
        },
        "distance_km":     distance_km,
        "estimated_fare":  estimated_fare,
        "surge_multiplier": surge,
        "payment_mode":    random.choice(PAYMENT_MODES),
        "hour_of_day":     hour,
        "day_of_week":     now.strftime("%A"),
    }


def generate_ride_completed_event(request_event: dict) -> dict:
    wait_minutes    = round(random.uniform(2, 15), 1)
    actual_fare     = round(request_event["estimated_fare"] * random.uniform(0.9, 1.15), 2)
    ride_duration   = round(request_event["distance_km"] * random.uniform(3, 6), 1)

    return {
        "event_id":         str(uuid.uuid4()),
        "event_type":       "RIDE_COMPLETED",
        "timestamp":        datetime.utcnow().isoformat(),
        "ride_id":          request_event["ride_id"],
        "user_id":          request_event["user_id"],
        "driver_id":        f"DRV-{random.randint(1000, 9999)}",
        "ride_type":        request_event["ride_type"],
        "status":           random.choices(["COMPLETED", "CANCELLED"], weights=[85, 15])[0],
        "pickup_zone_id":   request_event["pickup"]["zone_id"],
        "dropoff_zone_id":  request_event["dropoff"]["zone_id"],
        "distance_km":      request_event["distance_km"],
        "duration_minutes": ride_duration,
        "wait_time_minutes": wait_minutes,
        "estimated_fare":   request_event["estimated_fare"],
        "actual_fare":      actual_fare,
        "surge_multiplier": request_event["surge_multiplier"],
        "payment_mode":     request_event["payment_mode"],
        "driver_rating":    round(random.uniform(3.0, 5.0), 1),
        "user_rating":      round(random.uniform(3.0, 5.0), 1),
        "cancellation_reason": random.choice(CANCELLATION_REASONS) if random.random() < 0.15 else None,
    }


def generate_driver_location_event() -> dict:
    zone = random.choice(CITY_ZONES)
    lat, lng = get_random_coords(zone)
    return {
        "event_id":   str(uuid.uuid4()),
        "event_type": "DRIVER_LOCATION",
        "timestamp":  datetime.utcnow().isoformat(),
        "driver_id":  f"DRV-{random.randint(1000, 9999)}",
        "latitude":   lat,
        "longitude":  lng,
        "zone_id":    zone["zone_id"],
        "zone_name":  zone["name"],
        "is_available": random.choice([True, False]),
        "current_speed_kmh": round(random.uniform(0, 60), 1),
    }


class RideEventProducer:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',                   # Wait for all replicas
            retries=3,
            linger_ms=5,                  # Small batching window
            compression_type='gzip',
        )
        logger.info("Kafka Producer initialized.")

    def send_event(self, topic: str, key: str, event: dict):
        future = self.producer.send(topic, key=key, value=event)
        try:
            record_metadata = future.get(timeout=10)
            logger.debug(
                f"Sent to {record_metadata.topic} "
                f"partition={record_metadata.partition} "
                f"offset={record_metadata.offset}"
            )
        except KafkaError as e:
            logger.error(f"Failed to send event: {e}")

    def run(self, duration_seconds: float = None):
        """
        Continuously produce ride events.
        :param duration_seconds: Run for this many seconds (None = forever).
        """
        logger.info(f"Starting event production at ~{EVENTS_PER_DAY} events/day")
        start = time.time()
        count = 0

        try:
            while True:
                # 1. Ride Request
                request = generate_ride_request_event()
                self.send_event(TOPIC_RIDE_REQUESTS, request["ride_id"], request)

                # 2. Ride Completed (slight delay simulation)
                completed = generate_ride_completed_event(request)
                self.send_event(TOPIC_RIDE_COMPLETED, completed["ride_id"], completed)

                # 3. Driver Location (multiple drivers)
                for _ in range(random.randint(1, 3)):
                    loc = generate_driver_location_event()
                    self.send_event(TOPIC_DRIVER_LOCATION, loc["driver_id"], loc)

                count += 1
                if count % 100 == 0:
                    elapsed = time.time() - start
                    logger.info(f"Produced {count} ride events in {elapsed:.1f}s")

                if duration_seconds and (time.time() - start) >= duration_seconds:
                    break

                time.sleep(SLEEP_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Producer stopped by user.")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Total events produced: {count}")


if __name__ == "__main__":
    producer = RideEventProducer()
    producer.run()
