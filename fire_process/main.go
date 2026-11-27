package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"main/hivemq"
	fire "main/httpPetition"
	"main/rabbit"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	amqp "github.com/rabbitmq/amqp091-go"
)

type FireProcess struct {
	hiveMQ        hivemq.HiveMqConn
	rabbitMQ      rabbit.RabbitConn
	windCh        chan hivemq.WindTelemetryPayload
	kafkaProducer *kafka.Producer
}

var VALOR_MAP = "PEPRITIONS"

func generarUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)

	// Versión 4 y variante RFC 4122
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (fp *FireProcess) PublishKafka(ctx context.Context, topic, key string, value []byte) error {
	if fp == nil || fp.kafkaProducer == nil {
		return fmt.Errorf("kafka producer no inicializado")
	}

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	t := topic
	err := fp.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &t,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(key),
		Value: value,
	}, deliveryChan)
	if err != nil {
		return fmt.Errorf("encolando mensaje en Kafka (%s): %w", topic, err)
	}

	select {
	case ev := <-deliveryChan:
		m, ok := ev.(*kafka.Message)
		if !ok {
			return fmt.Errorf("evento inesperado de Kafka: %T", ev)
		}
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("error entregando a %s: %w", topic, m.TopicPartition.Error)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout esperando ack de Kafka en %s", topic)
	}
}

func (fp *FireProcess) procesarTelemetriaViento(ctx context.Context, windCh <-chan hivemq.WindTelemetryPayload) {
	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-windCh:
			if !ok {
				return
			}

			log.Printf("[WIND] Foco %s | Sensor %s (%s) | Viento: %.1f km/h (dir %.0f°) | Temp: %.1f°C | Hum: %.0f%%",
				t.FireID, t.SensorID, t.Region, t.WindSpeed, t.WindDirection, t.Temperature, t.Humidity)

			payload, err := json.Marshal(t)
			if err != nil {
				log.Printf("[KAFKA] Error serializando telemetría a JSON: %v", err)

			}

			severity := "fires.severity."

			if t.WindSpeed >= 30.0 {
				if t.Humidity <= 25.0 {

					if t.Temperature >= 30.0 {
						if t.Rain <= 1 {
							severity += "extreme"
							//caso de extreme
						}
						//caso de hard
						severity += "hard"
					}
					//caso de medium
					severity += "medium"
				}
				//caso de low
				severity += "low"
			} else {
				severity += "low"
			}

			if err := fp.PublishKafka(ctx, severity, t.FireID, payload); err != nil {
				log.Printf("[KAFKA] Error publicando en fires.severity.low: %v", err)
			}
		}
	}
}

func runCycle(ctx context.Context, fire_process *FireProcess, msgs <-chan amqp.Delivery) error {

	corrID := generarUUID()

	message := "Asking for petitions"

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if fire_process.rabbitMQ.Channel == nil || fire_process.rabbitMQ.Channel.IsClosed() {
		return fmt.Errorf("canal de RabbitMQ cerrado o no inicializado")
	}

	log.Println("[Emisor] Enviando tarea...")
	err := fire_process.rabbitMQ.Channel.PublishWithContext(reqCtx,
		"",
		"scheduler_queue",
		false,
		false,
		amqp.Publishing{
			DeliveryMode:  amqp.Persistent,
			ContentType:   "text/plain",
			CorrelationId: corrID,
			ReplyTo:       fire_process.rabbitMQ.Queue.Name,
			Body:          []byte(message),
		},
	)
	if err != nil {
		return fmt.Errorf("error publicando mensaje: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-reqCtx.Done():
			return fmt.Errorf("[Emisor] Timeout esperando respuesta del worker")
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("canal de mensajes cerrado")
			}
			if corrID == d.CorrelationId {
				valorStr := strings.TrimSpace(string(d.Body))
				valor, err := strconv.Atoi(valorStr)
				if err != nil {
					return fmt.Errorf("respuesta no numérica (%q): %w", valorStr, err)
				}

				log.Printf("[Emisor] Respuesta recibida -> %d", valor)

				if valor > 0 {
					fires, err := fire.FirePetition(valor)

					if err != nil {
						log.Printf("Error en las peticiones de FirePetition %v", err)
					}

					if len(fires) > 0 {

						// Aqui gestionamos como de peligroso parece el incendio
						for idx, f := range fires {
							log.Printf("Foco #%d -> lat=%.4f lon=%.4f FRP=%.1f conf=%s",
								idx+1, f.Latitude, f.Longitude, f.FRP, f.Confidence)

							//if f.Confidence == "h" {
							fire_process.hiveMQ.PublishFireActivate(f.Latitude, f.Longitude)

							err := fire_process.hiveMQ.SubscribeWindForFire(f.Latitude, f.Longitude, func(t hivemq.WindTelemetryPayload) {
								select {
								case <-ctx.Done():
								case fire_process.windCh <- t:
								}
							})

							if err != nil {
								log.Printf("Error aceptando peticiones de los sensores %v", err)
							}
							//}
						}
					}

				}
				return nil
			}
		}
	}
}

func handleRabbitPetition(ctx context.Context) {

	kafkaBrokers := "localhost:9092"

	kp, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"acks":              "1",
	})
	if err != nil {
		log.Fatalf("error creando kafka producer: %v", err)
	}
	defer func() {
		kp.Flush(5000)
		kp.Close()
	}()

	hiveMq, err := hivemq.NewMQTTClient(VALOR_MAP)
	if err != nil {
		log.Fatalf("Error al iniciarse el broker de HIveMQ: %v", err)
	}

	defer hiveMq.Close(VALOR_MAP)

	r, err := rabbit.NewRabbitConn()
	if err != nil {
		log.Fatalf("Fallo al iniciar RabbitMQ: %v", err)
	}
	defer r.Close()

	msgs, err := r.Channel.Consume(r.Queue.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error consumiendo respuestas: %v", err)
	}

	// declaramos la estructura de FireProcess
	fireProcess := &FireProcess{
		hiveMQ:        *hiveMq,
		rabbitMQ:      *r,
		windCh:        make(chan hivemq.WindTelemetryPayload, 100),
		kafkaProducer: kp,
	}

	go fireProcess.procesarTelemetriaViento(ctx, fireProcess.windCh)

	for {
		if err := runCycle(ctx, fireProcess, msgs); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Error en ciclo: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	handleRabbitPetition(ctx)
	log.Println("Aplicación detenida limpiamente.")
}
