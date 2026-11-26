import json
import os
import sqlite3
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

BROKER_HOST = "localhost"
BROKER_PORT = 1883
TOPIC_SUBSCRIPTION = "spain/#"
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fires.db")


def init_db():
    # Initialize SQLite database in WAL mode with active state and history tables
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS active_fires (
            fire_id TEXT PRIMARY KEY,
            sensor_id TEXT,
            region TEXT,
            lat REAL,
            lon REAL,
            active INTEGER DEFAULT 1,
            temperature REAL,
            humidity REAL,
            wind_speed REAL,
            wind_direction REAL,
            rain REAL,
            danger_level TEXT,
            simulated INTEGER DEFAULT 0,
            updated_at TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reading_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fire_id TEXT,
            sensor_id TEXT,
            region TEXT,
            lat REAL,
            lon REAL,
            temperature REAL,
            humidity REAL,
            wind_speed REAL,
            wind_direction REAL,
            rain REAL,
            danger_level TEXT,
            simulated INTEGER DEFAULT 0,
            recorded_at TEXT
        )
        """
    )

    conn.commit()
    return conn


def handle_sensor_control(conn, payload):
    # Update activation status and coordinates when a sensor wake/sleep command arrives
    now = datetime.now(timezone.utc).isoformat()
    fire_id = payload.get("fire_id")
    if not fire_id:
        return

    is_active = 1 if payload.get("active", True) else 0
    conn.execute(
        """
        INSERT INTO active_fires (fire_id, sensor_id, region, lat, lon, active, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fire_id) DO UPDATE SET
            sensor_id = excluded.sensor_id,
            region = excluded.region,
            lat = excluded.lat,
            lon = excluded.lon,
            active = excluded.active,
            updated_at = excluded.updated_at
        """,
        (
            fire_id,
            payload.get("sensor_id"),
            payload.get("region", "Spain"),
            payload.get("lat"),
            payload.get("lon"),
            is_active,
            now,
        ),
    )
    conn.commit()
    print(f"[SENSOR CONTROL] Fire {fire_id} -> active={bool(is_active)}")


def handle_fire_status(conn, payload):
    # Upsert current telemetry into active_fires and append a record to reading_history
    now = datetime.now(timezone.utc).isoformat()
    fire_id = payload.get("fire_id")
    if not fire_id:
        return

    is_simulated = 1 if payload.get("simulated", False) else 0
    values = (
        fire_id,
        payload.get("sensor_id"),
        payload.get("region", "Spain"),
        payload.get("lat"),
        payload.get("lon"),
        payload.get("temperature"),
        payload.get("humidity"),
        payload.get("wind_speed"),
        payload.get("wind_direction"),
        payload.get("rain"),
        payload.get("danger_level", "UNKNOWN"),
        is_simulated,
        now,
    )

    conn.execute(
        """
        INSERT INTO active_fires (
            fire_id, sensor_id, region, lat, lon, active,
            temperature, humidity, wind_speed, wind_direction,
            rain, danger_level, simulated, updated_at
        )
        VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fire_id) DO UPDATE SET
            sensor_id = excluded.sensor_id,
            region = excluded.region,
            lat = excluded.lat,
            lon = excluded.lon,
            active = 1,
            temperature = excluded.temperature,
            humidity = excluded.humidity,
            wind_speed = excluded.wind_speed,
            wind_direction = excluded.wind_direction,
            rain = excluded.rain,
            danger_level = excluded.danger_level,
            simulated = excluded.simulated,
            updated_at = excluded.updated_at
        """,
        values,
    )

    conn.execute(
        """
        INSERT INTO reading_history (
            fire_id, sensor_id, region, lat, lon,
            temperature, humidity, wind_speed, wind_direction,
            rain, danger_level, simulated, recorded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )
    conn.commit()
    print(
        f"[TELEMETRY SAVED] {fire_id} ({payload.get('region')}) | "
        f"Temp: {payload.get('temperature')}°C | Wind: {payload.get('wind_speed')} m/s | "
        f"Danger: {payload.get('danger_level')}"
    )


def main():
    conn = init_db()

    def on_connect(client, userdata, flags, reason_code, properties=None):
        print(f"Connected to HiveMQ ({BROKER_HOST}:{BROKER_PORT}) with code: {reason_code}")
        client.subscribe(TOPIC_SUBSCRIPTION, qos=1)
        print(f"Subscribed to topic: {TOPIC_SUBSCRIPTION}\n")

    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            if msg.topic.startswith("spain/sensors/"):
                handle_sensor_control(conn, payload)
            elif msg.topic.startswith("spain/fireStatus/"):
                handle_fire_status(conn, payload)
        except json.JSONDecodeError:
            print(f"Ignored non-JSON message on {msg.topic}")
        except Exception as e:
            print(f"Error processing message on {msg.topic}: {e}")

    if hasattr(mqtt, "CallbackAPIVersion"):
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    else:
        client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nStopping storage subscriber...")
    finally:
        client.disconnect()
        conn.close()


if __name__ == "__main__":
    main()
