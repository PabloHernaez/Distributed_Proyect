import json
import os
import sqlite3
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

BROKER_HOST = "localhost"
BROKER_PORT = 1883
TOPICS = [
    ("monitoring/#", 1),
    ("ssdd_espana_incendios_2026/#", 1),
    ("spain/#", 1),
]
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fires.db")


def init_db():
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


def calcular_peligro(payload):
    if payload.get("danger_level"):
        return str(payload["danger_level"]).upper()
    temp = float(payload.get("temperature") or 0)
    hum = float(payload.get("humidity") or 100)
    wind = float(payload.get("wind_speed") or 0)

    if (temp >= 35 and hum <= 25) or wind >= 40:
        return "EXTREME"
    if (temp >= 30 and hum <= 35) or wind >= 30:
        return "HIGH"
    if temp >= 22 or wind >= 20:
        return "MEDIUM"
    return "LOW"


def handle_sensor_control(conn, payload):
    now = datetime.now(timezone.utc).isoformat()
    lat = payload.get("lat")
    lon = payload.get("lon")
    fire_id = payload.get("fire_id") or (f"fire_{lat}_{lon}" if lat is not None and lon is not None else None)
    if not fire_id:
        return

    action = str(payload.get("action") or "").upper()
    if action == "EXTINGUISHED":
        is_active = 0
    elif "active" in payload:
        is_active = 1 if payload["active"] else 0
    else:
        is_active = 1

    conn.execute(
        """
        INSERT INTO active_fires (fire_id, sensor_id, region, lat, lon, active, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fire_id) DO UPDATE SET
            sensor_id = COALESCE(excluded.sensor_id, active_fires.sensor_id),
            region = COALESCE(excluded.region, active_fires.region),
            lat = COALESCE(excluded.lat, active_fires.lat),
            lon = COALESCE(excluded.lon, active_fires.lon),
            active = excluded.active,
            updated_at = excluded.updated_at
        """,
        (
            fire_id,
            payload.get("sensor_id"),
            payload.get("region", "Spain"),
            lat,
            lon,
            is_active,
            now,
        ),
    )
    conn.commit()
    print(f"[ALERT CONTROL] Fire {fire_id} -> active={bool(is_active)}")


def handle_fire_status(conn, payload):
    now = datetime.now(timezone.utc).isoformat()
    lat = payload.get("lat")
    lon = payload.get("lon")
    fire_id = payload.get("fire_id") or (f"fire_{lat}_{lon}" if lat is not None and lon is not None else None)
    if not fire_id:
        return

    danger = calcular_peligro(payload)
    is_simulated = 1 if payload.get("simulated", False) else 0
    values = (
        fire_id,
        payload.get("sensor_id"),
        payload.get("region", "Spain"),
        lat,
        lon,
        payload.get("temperature"),
        payload.get("humidity"),
        payload.get("wind_speed"),
        payload.get("wind_direction"),
        payload.get("rain"),
        danger,
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
            lat = COALESCE(active_fires.lat, excluded.lat),
            lon = COALESCE(active_fires.lon, excluded.lon),
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
        f"[TELEMETRY SAVED] {fire_id} ({payload.get('region', 'Spain')}) | "
        f"Temp: {payload.get('temperature')}°C | Wind: {payload.get('wind_speed')} km/h | "
        f"Danger: {danger}"
    )


def main():
    conn = init_db()

    def on_connect(client, userdata, flags, reason_code, properties=None):
        print(f"Connected to HiveMQ ({BROKER_HOST}:{BROKER_PORT}) with code: {reason_code}")
        client.subscribe(TOPICS)
        print(f"Subscribed to topics: {[t[0] for t in TOPICS]}\n")

    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            topic = msg.topic
            if topic in ("monitoring/fires", "ssdd_espana_incendios_2026/alertas") or topic.startswith("spain/sensors/"):
                handle_sensor_control(conn, payload)
            elif (
                topic.startswith("monitoring/sensors/")
                or topic == "ssdd_espana_incendios_2026/telemetria"
                or topic.startswith("spain/fireStatus/")
            ):
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
