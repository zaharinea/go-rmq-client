package rmqclient

import (
	"context"

	"github.com/streadway/amqp"
)

// Producer struct
type Producer struct {
	Connection
	exchanges map[string]*Exchange
}

// NewProducer returns a new Producer struct
func NewProducer(uri string, logger Logger) *Producer {
	exchanges := make(map[string]*Exchange)
	err := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	return &Producer{
		exchanges: exchanges,
		Connection: Connection{
			uri:              uri,
			err:              err,
			ctx:              ctx,
			notifyQuit:       cancel,
			reconnectTimeout: reconnectTimeout,
			logger:           logger,
		},
	}
}

//RegisterExchange register exchange
func (p *Producer) RegisterExchange(exchange *Exchange) {
	if _, exist := p.exchanges[exchange.Name]; exist {
		p.logger.Fatalf("Exchange already registred: %s", exchange.Name)
	}
	p.exchanges[exchange.Name] = exchange
}

//Start start Producer
func (p *Producer) Start() {
	err := p.connect()
	if err != nil {
		p.logger.Fatal("Failed connect", err)
	}

	err = p.setupChanels()
	if err != nil {
		p.logger.Fatal("Failed setup Channel", err)
	}
}

//Stop stop Producer
func (p *Producer) Stop() {
	p.Close()
}

func (p *Producer) reconnect() error {
	if err := p.connect(); err != nil {
		return err
	}
	if err := p.setupChanels(); err != nil {
		return err
	}
	return nil
}

//Publish send message
func (p *Producer) Publish(exchangeName string, routingKey string, data []byte, priority uint8) error {
	select {
	case err := <-p.err:
		if err != nil {
			p.reconnect()
		}
	default:
	}

	err := p.channel.Publish(
		exchangeName,
		routingKey,
		false, // mandatory - we don't care if there I no queue
		false, // immediate - we don't care if there is no consumer on the queue
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         data,
			DeliveryMode: amqp.Persistent,
			Priority:     priority,
		})
	if err != nil {
		p.logger.Errorf("Failed publish to rabbitmq: %s", err)
		return err
	}
	return nil
}
