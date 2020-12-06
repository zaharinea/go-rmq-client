package rmqclient

import (
	"context"
	"fmt"

	"github.com/streadway/amqp"
)

const (
	defaultCountWorkers = 1
	defaultMultiplier   = 1
)

// HandlerFunc defines the handler
type HandlerFunc func(context.Context, amqp.Delivery) bool

// Queue struct
type Queue struct {
	Name          string
	RoutingKey    string
	Durable       bool
	AutoDelete    bool
	Exclusive     bool
	NoWait        bool
	Arguments     amqp.Table
	requeue       bool
	prefetchCount int
	handler       HandlerFunc
	deliveries    chan amqp.Delivery
	countWorkers  int
}

// NewQueue returns a new Queue struct
func NewQueue(name string, routingKey string, arguments amqp.Table) *Queue {
	deliveries := make(chan amqp.Delivery)
	return &Queue{
		Name:          name,
		RoutingKey:    routingKey,
		Durable:       true,
		AutoDelete:    false,
		Exclusive:     false,
		NoWait:        false,
		Arguments:     arguments,
		requeue:       false,
		prefetchCount: defaultCountWorkers * defaultMultiplier,
		deliveries:    deliveries,
		countWorkers:  defaultCountWorkers,
	}
}

// SetHandler register handler in Queue
func (q *Queue) SetHandler(handler HandlerFunc) *Queue {
	q.handler = handler
	return q
}

// SetRequeue set requeue param
func (q *Queue) SetRequeue(value bool) *Queue {
	q.requeue = value
	return q
}

// SetCountWorkers set count of workers
func (q *Queue) SetCountWorkers(value int) *Queue {
	q.countWorkers = value
	q.prefetchCount = value * defaultMultiplier
	return q
}

// SetPrefetchCount set prefetch count
func (q *Queue) SetPrefetchCount(value int) *Queue {
	q.prefetchCount = value
	return q
}

func (q *Queue) declare(channel *amqp.Channel) error {
	_, err := channel.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, q.Arguments)
	if err != nil {
		return fmt.Errorf("Failed to declare a queue %s: %w", q.Name, err)
	}
	return nil
}

func (q *Queue) consume(channel *amqp.Channel) error {
	err := channel.Qos(q.prefetchCount, 0, false)
	if err != nil {
		return fmt.Errorf("Error setting qos: %w", err)
	}

	deliveries, err := channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return fmt.Errorf("Queue Consume: %w", err)
	}

	go func() {
		for delivery := range deliveries {
			q.deliveries <- delivery
		}
	}()

	return nil
}
