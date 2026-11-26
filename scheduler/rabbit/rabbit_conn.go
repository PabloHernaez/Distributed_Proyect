package rabbit

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitConn struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
	Queue   amqp.Queue
}

// Empieza en mayúscula para poder llamarla desde cualquier paquete
func NewRabbitConn() (*RabbitConn, error) {
	conn, err := amqp.Dial("amqp://admin:TuPasswordSegura123@localhost:5672/")
	if err != nil {
		return nil, fmt.Errorf("error conectando a RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close() // Solo cerramos si falla este paso
		return nil, fmt.Errorf("error abriendo canal: %w", err)
	}

	queue, err := ch.QueueDeclare("scheduler_queue", true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("error declarando cola: %w", err)
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("Error configurando QoS: %v", err)
		return nil, fmt.Errorf("error declarando quality of service: %w", err)
	}

	// NO hacemos defer Close() aquí porque necesitamos que sigan abiertos
	return &RabbitConn{
		Conn:    conn,
		Channel: ch,
		Queue:   queue,
	}, nil
}

// Método auxiliar para cerrar todo limpiamente desde fuera
func (r *RabbitConn) Close() {
	if r.Channel != nil {
		r.Channel.Close()
	}
	if r.Conn != nil {
		r.Conn.Close()
	}
}
