package rmqclient

import (
	"fmt"

	"github.com/streadway/amqp"
)

// Exchange struct
type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  amqp.Table
	Queues     []*Queue
}

// NewExchange returns a new Exchange struct
func NewExchange(name string, exchangeType string, arguments amqp.Table, queues []*Queue) *Exchange {
	return &Exchange{
		Name:       name,
		Type:       exchangeType,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Arguments:  arguments,
		Queues:     queues,
	}
}

func (e *Exchange) declareAndBind(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(e.Name, e.Type, e.Durable, e.AutoDelete, e.Internal, e.NoWait, e.Arguments); err != nil {
		return fmt.Errorf("Failed to declare an exchange %s: %s", e.Name, err)
	}

	for _, queue := range e.Queues {
		if err := channel.QueueBind(queue.Name, queue.RoutingKey, e.Name, false, nil); err != nil {
			return fmt.Errorf("Failed to bind a queue %s to exchange %s with routing key %s: %s", queue.Name, e.Name, queue.RoutingKey, err)
		}
	}
	return nil
}
