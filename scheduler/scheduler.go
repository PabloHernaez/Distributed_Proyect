package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"scheduler/persistance"
	"scheduler/rabbit"
	"strconv"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Scheduler struct {
	databse *persistance.Persistance
	rabbit  *rabbit.RabbitConn
}

func (s *Scheduler) databaseCounterResetInit(ctx context.Context) {
	// Ejecución inmediata al arrancar
	s.resetDatabaseCounter(ctx)

	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Deteniendo bucle de reseteo de contador...")
			return
		case <-ticker.C:
			s.resetDatabaseCounter(ctx)
		}
	}
}

func (s *Scheduler) resetDatabaseCounter(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	err := s.databse.SetCounter(ctx, "global", 5000)
	if err != nil {
		log.Printf("Error reseteando contador en DB: %v", err)
	}
}

func (s *Scheduler) Close() {
	if s.databse != nil {
		s.databse.Close()
	}
	if s.rabbit != nil {
		s.rabbit.Close()
	}
}

func main() {
	// Se cancela al recibir Ctrl+C (SIGINT) o SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rabbitConn, err := rabbit.NewRabbitConn()
	if err != nil {
		log.Fatalf("Fallo al iniciar RabbitMQ: %v", err)
	}

	repo, err := persistance.NewPersistance(ctx, "postgresql://root@localhost:26257/queue_system?sslmode=disable")
	if err != nil {
		rabbitConn.Close()
		log.Fatal(err)
	}

	scheduler := &Scheduler{databse: repo, rabbit: rabbitConn}
	defer scheduler.Close()

	// Se ejecuta en segundo plano para no bloquear el consumidor de RabbitMQ
	go scheduler.databaseCounterResetInit(ctx)

	msgs, err := scheduler.rabbit.Channel.Consume(rabbitConn.Queue.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error registrando consumidor: %v", err)
	}

	log.Println("[Worker] Esperando mensajes. Pulsa Ctrl+C para salir.")

	for {
		select {
		case <-ctx.Done():
			log.Println("[Worker] Apagando scheduler...")
			return
		case d, ok := <-msgs:
			if !ok {
				log.Println("[Worker] Canal de mensajes cerrado.")
				return
			}

			tarea := string(d.Body)
			log.Printf("[Worker] Recibido: '%s'. Procesando...", tarea)

			valor, err := scheduler.databse.GetCounter(ctx)
			if err != nil {
				log.Printf("We cannot fetch the counter data")
				return
			}

			executions := 5

			if valor >= executions {
				executions = 5
			} else if valor <= 0 {
				executions = 0
			} else {
				executions = valor
			}

			// Aqui llamamos al metodo que envia la peticion de vuelta
			_, err = scheduler.databse.DecrementCounter(ctx, "global", executions)

			// Usamos pubCtx distinto para no sobreescribir el ctx principal
			pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = scheduler.rabbit.Channel.PublishWithContext(pubCtx,
				"",        // exchange por defecto
				d.ReplyTo, // cola de retorno del emisor
				false,
				false,
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(strconv.Itoa(executions)),
				},
			)
			cancel()

			if err != nil {
				log.Printf("Error enviando respuesta: %v", err)
				continue
			}

			d.Ack(false)
			log.Println("[Worker] Tarea completada y respuesta enviada.")

			if errors.Is(err, persistance.ErrSaldoInsuficiente) {
				log.Printf("[Worker] Saldo insuficiente para tarea: %s", tarea)
				// Considera hacer d.Nack(false, false) o responder con error según tu flujo
				continue
			} else if err != nil {
				log.Printf("[Worker] Error decrementando contador: %v", err)
				continue
			}
		}
	}
}
