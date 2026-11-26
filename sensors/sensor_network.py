from datetime import datetime, timezone
import json
import math
import random
import threading
import time
import paho.mqtt.client as mqtt
import requests

# Broker MQTT y parámetros generales
BROKER = "localhost"
PORT = 1883

TOPIC_ALERTAS = "ssdd_espana_incendios_2026/alertas"
TOPIC_TELEMETRIA = "ssdd_espana_incendios_2026/telemetria"
TOPIC_WIND = "sensors/wind/"

INTERVALO_ENVIO_SEG = 15       # Cada cuánto emite telemetría un sensor activo
CACHE_OPEN_METEO_SEG = 900    # 15 min de caché por sensor para no saturar la API
TTL_INCENDIO_SEG = 600        # Si no llega renovación en 10 min, el foco caduca
PROBABILIDAD_FALLO = 0.05     # 5% de caída simulada por ciclo para probar vecinos


def generar_red_sensores():
    # Grupos de 4 sensores (~6-10 km entre ellos) en zonas forestales de España
    zonas_riesgo = [
        ("Galicia", "GAL", 42.34, -7.86),
        ("Castilla y León", "CYL", 41.80, -6.30),
        ("Asturias", "AST", 43.15, -6.55),
        ("Madrid", "MAD", 40.75, -3.95),
        ("Castilla-La Mancha", "CLM", 39.35, -4.35),
        ("Comunitat Valenciana", "VAL", 39.75, -0.85),
        ("Andalucía", "AND", 37.90, -4.80),
        ("Andalucía", "AND2", 36.75, -5.15),
        ("Extremadura", "EXT", 40.25, -6.10),
        ("Cataluña", "CAT", 41.70, 1.85),
        ("Aragón", "ARA", 40.35, -0.75),
        ("Canarias", "CAN", 28.30, -16.60),
    ]

    sensores = []
    offsets = [(0.0, 0.0), (0.06, 0.04), (-0.05, 0.06), (0.03, -0.07)]

    for region, prefijo, base_lat, base_lon in zonas_riesgo:
        for idx, (dlat, dlon) in enumerate(offsets, start=1):
            sensores.append({
                "sensor_id": f"SENS-{prefijo}-{idx:02d}",
                "region": region,
                "lat": round(base_lat + dlat, 4),
                "lon": round(base_lon + dlon, 4),
                "incendios_activos": {},  # fire_id -> timestamp de expiración
                "cache_clima": None,
                "cache_timestamp": 0,
            })
    return sensores


SENSORES = generar_red_sensores()
lock_sensores = threading.Lock()


def distancia_haversine_km(lat1, lon1, lat2, lon2):
    radio_tierra_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radio_tierra_km * c


def obtener_datos_open_meteo(sensor):
    ahora = time.time()
    if (
        sensor["cache_clima"] is not None
        and (ahora - sensor["cache_timestamp"]) < CACHE_OPEN_METEO_SEG
    ):
        return sensor["cache_clima"]

    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={sensor['lat']}&longitude={sensor['lon']}"
        "&current=temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,rain"
    )
    try:
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        data = resp.json()["current"]
        clima = {
            "temperature": data["temperature_2m"],
            "humidity": data["relative_humidity_2m"],
            "wind_speed": data["wind_speed_10m"],
            "wind_direction": data["wind_direction_10m"],
            "rain": data["rain"],
        }
        sensor["cache_clima"] = clima
        sensor["cache_timestamp"] = ahora
        return clima
    except Exception as e:
        print(f"[WARNING] Open-Meteo API error on {sensor['sensor_id']}: {e}")
        return sensor["cache_clima"]


def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"[CONNECTED] HiveMQ broker ({BROKER}). {len(SENSORES)} sensors initialized in standby mode.")
    client.subscribe(TOPIC_ALERTAS)
    print(f"[SUBSCRIBED] Listening for fire alerts on: {TOPIC_ALERTAS}")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        return

    fire_id = payload.get("fire_id")
    action = str(payload.get("action") or "ACTIVATE").upper()

    if not fire_id:
        return

    with lock_sensores:
        if action == "EXTINGUISHED":
            apagados = 0
            for s in SENSORES:
                if fire_id in s["incendios_activos"]:
                    del s["incendios_activos"][fire_id]
                    apagados += 1
            print(f"[EXTINGUISHED] Fire {fire_id} closed. {apagados} sensors returned to sleep mode.")
            return

        if action != "ACTIVATE":
            print(f"[WARNING] Unknown action '{action}' ignored for {fire_id}")
            return

        lat = payload.get("lat")
        lon = payload.get("lon")
        radius_km = payload.get("radius_km")

        if lat is None or lon is None or radius_km is None:
            print(f"[WARNING] Incomplete alert payload ignored: {payload}")
            return

        despiertos = []
        expiracion = time.time() + TTL_INCENDIO_SEG


        for s in SENSORES:
            dist = distancia_haversine_km(s["lat"], s["lon"], float(lat), float(lon))
            if dist <= float(radius_km):
                s["incendios_activos"][fire_id] = expiracion
                despiertos.append(f"{s['sensor_id']} ({dist:.1f} km)")

        if despiertos:
            print(f"[ALERT] {fire_id} (Radius: {radius_km} km) -> Waking up {len(despiertos)} sensors: {', '.join(despiertos)}")
        else:
            print(f"[ALERT] {fire_id} at ({lat}, {lon}) with radius {radius_km} km: No sensors found in range.")

def bucle_telemetria(client):
    while True:
        ahora = time.time()
        sensores_a_reportar = []

        with lock_sensores:
            for s in SENSORES:
                expirados = [fid for fid, exp in s["incendios_activos"].items() if ahora > exp]
                for fid in expirados:
                    del s["incendios_activos"][fid]
                    print(f"[SLEEP] Sensor {s['sensor_id']} returned to standby due to timeout ({fid}).")

                if s["incendios_activos"]:
                    sensores_a_reportar.append(s)

        for s in sensores_a_reportar:
            if random.random() < PROBABILIDAD_FALLO:
                print(f"[SIMULATED FAILURE] {s['sensor_id']} failed to respond in this cycle.")
                continue

            clima = obtener_datos_open_meteo(s)
            if not clima:
                continue

            # Pequeña pausa aleatoria para escalonar peticiones a la API
            time.sleep(random.uniform(0.2, 0.8))

            for fire_id in list(s["incendios_activos"].keys()):
                mensaje = {
                    "fire_id": fire_id,
                    "sensor_id": s["sensor_id"],
                    "region": s["region"],
                    "lat": s["lat"],
                    "lon": s["lon"],
                    "temperature": clima["temperature"],
                    "humidity": clima["humidity"],
                    "wind_speed": clima["wind_speed"],
                    "wind_direction": clima["wind_direction"],
                    "rain": clima["rain"],
                    "simulated": 0,
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }

                wind = f"{TOPIC_WIND}{fire_id}"

                client.publish(wind, json.dumps(mensaje))
                client.publish(TOPIC_TELEMETRIA, json.dumps(mensaje))
                print(
                    f"[TELEMETRY] {s['sensor_id']} -> {fire_id} | "
                    f"Temp: {clima['temperature']}°C | Humidity: {clima['humidity']}% | "
                    f"Wind: {clima['wind_speed']} km/h"
                )

        time.sleep(INTERVALO_ENVIO_SEG)


if __name__ == "__main__":
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT, 60)

    hilo_envio = threading.Thread(target=bucle_telemetria, args=(client,), daemon=True)
    hilo_envio.start()

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[STOP] Shutting down sensor network...")
        client.disconnect()
