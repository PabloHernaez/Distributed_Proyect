package hivemq

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	defaultHiveMQBroker = "tcp://localhost:1883"
	mqttAlertasTopic    = "ssdd_espana_incendios_2026/alertas"
	activateRadiusKm    = 100.0
)

// Satelites disponibles
// VIIRS_SNPP_NRT	Suomi-NPP
// VIIRS_NOAA20_NRT	NOAA-20
// VIIRS_NOAA21_NRT	NOAA-21
// MODIS_NRT	MODIS
// MODIS_SP	MODIS, procesamiento estándar

type fireAlertPayload struct {
	FireID   string  `json:"fire_id"`
	Action   string  `json:"action"`
	Lat      float64 `json:"lat"`
	Lon      float64 `json:"lon"`
	RadiusKm float64 `json:"radius_km"`
}

type LWTStatusPayload struct {
	ClientID string `json:"client_id"`
	Status   string `json:"status"`
	Reason   string `json:"reason"`
}

type WindTelemetryPayload struct {
	FireID        string  `json:"fire_id"`
	SensorID      string  `json:"sensor_id"`
	Region        string  `json:"region"`
	SensorLat     float64 `json:"sensor_lat"`
	SensorLon     float64 `json:"sensor_lon"`
	WindSpeed     float64 `json:"wind_speed"`
	WindDirection float64 `json:"wind_direction"`
	Temperature   float64 `json:"temperature"`
	Humidity      float64 `json:"humidity"`
	Rain          float64 `json:"rain"`
	Timestamp     string  `json:"timestamp"`
}

type HiveMqConn struct {
	Client mqtt.Client
}

func NewMQTTClient(clientID string) (*HiveMqConn, error) {
	lwtTopic := fmt.Sprintf("monitoring/fires/%s/status", clientID)
	lwtBytes, err := json.Marshal(LWTStatusPayload{
		ClientID: clientID,
		Status:   "offline",
		Reason:   "unexpected_disconnect",
	})
	if err != nil {
		return nil, fmt.Errorf("serializando LWT: %w", err)
	}

	opts := mqtt.NewClientOptions().
		AddBroker(defaultHiveMQBroker).
		SetClientID(clientID).
		SetCleanSession(false).
		SetAutoReconnect(true).
		SetKeepAlive(30*time.Second).
		SetWill(lwtTopic, string(lwtBytes), 1, true)

	opts.OnConnect = func(c mqtt.Client) {
		log.Printf("[MQTT] Conectado a HiveMQ CE (%s) como %s", defaultHiveMQBroker, clientID)
		onlineBytes, _ := json.Marshal(LWTStatusPayload{
			ClientID: clientID,
			Status:   "online",
			Reason:   "connected",
		})
		c.Publish(lwtTopic, 1, true, onlineBytes)
	}

	opts.OnConnectionLost = func(_ mqtt.Client, err error) {
		log.Printf("[MQTT] Conexión perdida con HiveMQ CE: %v", err)
	}

	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()
	if token.Error() != nil {
		return nil, fmt.Errorf("conectando a HiveMQ CE en %s: %w", defaultHiveMQBroker, token.Error())
	}

	return &HiveMqConn{Client: client}, nil
}

func (h *HiveMqConn) Close(clientId string) {
	if h == nil || h.Client == nil {
		return
	}

	lwtTopic := fmt.Sprintf("monitoring/fires/%s/status", clientId)
	offlineBytes, err := json.Marshal(LWTStatusPayload{
		ClientID: clientId,
		Status:   "offline",
		Reason:   "graceful_shutdown",
	})
	if err == nil && h.Client.IsConnected() {
		pubToken := h.Client.Publish(lwtTopic, 1, true, offlineBytes)
		pubToken.Wait()
	}

	h.Client.Disconnect(250)
}

func (h *HiveMqConn) PublishFireActivate(lat float64, lon float64) error {
	if h == nil || h.Client == nil {
		return fmt.Errorf("conexión MQTT no inicializada")
	}

	payload, err := json.Marshal(fireAlertPayload{
		FireID:   fmt.Sprintf("%.4f&%.4f", lat, lon),
		Action:   "ACTIVATE",
		Lat:      lat,
		Lon:      lon,
		RadiusKm: activateRadiusKm,
	})
	if err != nil {
		return fmt.Errorf("serializando alerta MQTT: %w", err)
	}

	token := h.Client.Publish(mqttAlertasTopic, 1, false, payload)
	token.Wait()
	if token.Error() != nil {
		return fmt.Errorf("publicando en %s: %w", mqttAlertasTopic, token.Error())
	}
	return nil
}

func (h *HiveMqConn) SubscribeWindForFire(lat, lon float64, onTelemetry func(WindTelemetryPayload)) error {
	if h == nil || h.Client == nil {
		return fmt.Errorf("conexión MQTT no inicializada")
	}

	coordTag := fmt.Sprintf("%.4f&%.4f", lat, lon)
	topic := fmt.Sprintf("sensors/wind/%s", coordTag)

	token := h.Client.Subscribe(topic, 1, func(_ mqtt.Client, msg mqtt.Message) {
		var data WindTelemetryPayload
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			log.Printf("[MQTT] Error parseando telemetría en %s: %v", msg.Topic(), err)
			return
		}
		onTelemetry(data)
	})
	token.Wait()
	if token.Error() != nil {
		return fmt.Errorf("suscribiendo a %s: %w", topic, token.Error())
	}

	log.Printf("[MQTT] Listener activo en %s", topic)
	return nil
}
