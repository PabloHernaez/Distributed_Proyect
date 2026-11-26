package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const monitoringForwardTopic = "monitoring"

func main() {
	brokers := "localhost:9092"
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = envBrokers
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Producer compartido para reenviar mensajes recibidos al topic "monitoring"
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "1", // En producción multi-broker con RF>=3 suele usarse "all"
	})
	if err != nil {
		log.Fatalf("error creando producer de monitoring: %v", err)
	}
	defer producer.Close()

	var wg sync.WaitGroup

	// 1. Severidad baja y media -> procesa y reenvía a "monitoring"
	startConsumer(ctx, &wg, brokers, "cg-fires-low-medium", []string{
		"fires.severity.low",
		"fires.severity.medium",
	}, func(msg *kafka.Message) error {
		if err := handleLowMedium(msg); err != nil {
			return err
		}
		return forwardToMonitoring(ctx, producer, msg)
	})

	// 2. Severidad alta y extrema -> procesa y reenvía a "monitoring"
	startConsumer(ctx, &wg, brokers, "cg-fires-high-extreme", []string{
		"fires.severity.high",
		"fires.severity.extreme",
	}, func(msg *kafka.Message) error {
		if err := handleHighExtreme(msg); err != nil {
			return err
		}
		return forwardToMonitoring(ctx, producer, msg)
	})

	// 3. Monitorización -> solo consume (evita bucle hacia sí mismo)
	startConsumer(ctx, &wg, brokers, "cg-fires-monitoring", []string{
		"fires.monitoring",
	}, handleMonitoring)

	log.Println("3 consumidores (confluent-kafka-go) iniciados con reenvío a 'monitoring'. Ctrl+C para salir.")
	<-ctx.Done()
	log.Println("Cerrando consumidores...")
	wg.Wait()
	producer.Flush(5000)
	log.Println("Consumidores detenidos.")
}

type handlerFunc func(msg *kafka.Message) error

func startConsumer(
	ctx context.Context,
	wg *sync.WaitGroup,
	brokers string,
	groupID string,
	topics []string,
	handle handlerFunc,
) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		log.Fatalf("[%s] error creando consumer: %v", groupID, err)
	}

	if err := c.SubscribeTopics(topics, nil); err != nil {
		_ = c.Close()
		log.Fatalf("[%s] error suscribiendo a topics: %v", groupID, err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if err := c.Close(); err != nil {
				log.Printf("[%s] error cerrando consumer: %v", groupID, err)
			}
		}()

		for {
			if ctx.Err() != nil {
				return
			}

			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				if err := handle(e); err != nil {
					log.Printf("[%s] error procesando/reenviando mensaje: %v", groupID, err)
					continue
				}
				if _, err := c.CommitMessage(e); err != nil {
					log.Printf("[%s] error haciendo commit: %v", groupID, err)
				}
			case kafka.Error:
				log.Printf("[%s] error de Kafka (%v): %v", groupID, e.Code(), e)
			}
		}
	}()
}

func forwardToMonitoring(ctx context.Context, producer *kafka.Producer, src *kafka.Message) error {
	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	headers := append([]kafka.Header{}, src.Headers...)
	headers = append(headers,
		kafka.Header{Key: "source_topic", Value: []byte(*src.TopicPartition.Topic)},
		kafka.Header{Key: "forwarded_at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)

	targetTopic := monitoringForwardTopic
	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &targetTopic,
			Partition: kafka.PartitionAny, // Kafka reparte según Key (zona geográfica)
		},
		Key:     src.Key,
		Value:   src.Value,
		Headers: headers,
	}, deliveryChan)
	if err != nil {
		return fmt.Errorf("encolando mensaje para monitoring: %w", err)
	}

	select {
	case ev := <-deliveryChan:
		m, ok := ev.(*kafka.Message)
		if !ok {
			return fmt.Errorf("evento inesperado al entregar a monitoring: %T", ev)
		}
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("error entregando a monitoring: %w", m.TopicPartition.Error)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout esperando confirmación de entrega a monitoring")
	}
}

func handleLowMedium(msg *kafka.Message) error {
	fmt.Printf("[LOW/MEDIUM] topic=%s partition=%d zoneKey=%s offset=%d payload=%s\n",
		*msg.TopicPartition.Topic, msg.TopicPartition.Partition, string(msg.Key), msg.TopicPartition.Offset, string(msg.Value))
	return nil
}

func handleHighExtreme(msg *kafka.Message) error {
	fmt.Printf("[HIGH/EXTREME ALERT] topic=%s partition=%d zoneKey=%s offset=%d payload=%s\n",
		*msg.TopicPartition.Topic, msg.TopicPartition.Partition, string(msg.Key), msg.TopicPartition.Offset, string(msg.Value))
	return nil
}

func handleMonitoring(msg *kafka.Message) error {
	fmt.Printf("[MONITORING] topic=%s partition=%d zoneKey=%s offset=%d payload=%s\n",
		*msg.TopicPartition.Topic, msg.TopicPartition.Partition, string(msg.Key), msg.TopicPartition.Offset, string(msg.Value))
	return nil
}
