package rmqclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

// Consumer struct
type Consumer struct {
	uri              string
	conn             *amqp.Connection
	channel          *amqp.Channel
	queues           map[string]*Queue
	exchanges        map[string]*Exchange
	err              chan error
	ctx              context.Context
	notifyQuit       context.CancelFunc
	reconnectTimeout time.Duration
	logger           Logger
}

// NewConsumer returns a new Consumer struct
func NewConsumer(uri string, logger Logger) *Consumer {
	exchanges := make(map[string]*Exchange)
	queues := make(map[string]*Queue)
	err := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{
		uri:              uri,
		exchanges:        exchanges,
		queues:           queues,
		err:              err,
		ctx:              ctx,
		notifyQuit:       cancel,
		reconnectTimeout: time.Second * 3,
		logger:           logger,
	}
}

//Start start consumer
func (c *Consumer) Start() {
	err := c.connect()
	if err != nil {
		c.logger.Fatal("Failed connect", err)
	}

	err = c.setupChanels()
	if err != nil {
		c.logger.Fatal("Failed setup Channel", err)
	}

	err = c.setupQueues()
	if err != nil {
		c.logger.Fatal("Failed setup queues", err)
	}

	err = c.setupExchanges()
	if err != nil {
		c.logger.Fatal("Failed setup exchanges", err)
	}

	err = c.consume()
	if err != nil {
		c.logger.Fatal("Failed setup consumers", err)
	}
}

//Close stop consumer
func (c *Consumer) Close() error {
	c.notifyQuit()

	err := c.channel.Close()
	if err != nil {
		return err
	}
	err = c.conn.Close()
	if err != nil {
		return err
	}
	c.logger.Info("Closing rabbitmq channels and connection")
	return nil
}

//RegisterQueue register queue
func (c *Consumer) RegisterQueue(queues ...*Queue) {
	for _, queue := range queues {
		if _, exist := c.queues[queue.Name]; exist {
			c.logger.Fatalf("Queue already registred: %s", queue.Name)
		}
		c.queues[queue.Name] = queue
	}
}

//RegisterExchange register exchange
func (c *Consumer) RegisterExchange(exchange *Exchange) {
	for _, queue := range exchange.Queues {
		c.RegisterQueue(queue)
	}

	if _, exist := c.exchanges[exchange.Name]; exist {
		c.logger.Fatalf("Exchange already registred: %s", exchange.Name)
	}
	c.exchanges[exchange.Name] = exchange
}

func (c *Consumer) connect() error {
	var err error
	for {
		c.logger.Info("Start connect to rabbitmq")
		c.conn, err = amqp.Dial(c.uri)
		if err == nil {
			go func() {
				<-c.conn.NotifyClose(make(chan *amqp.Error))
				c.err <- errors.New("Connection Closed")
			}()
			c.logger.Info("Success connect to rabbitmq")
			return nil
		}
		c.logger.Errorf("Failed connect to rabbitmq: %s", err.Error())
		time.Sleep(c.reconnectTimeout)

	}
}

func (c *Consumer) reconnect() error {
	if err := c.connect(); err != nil {
		return err
	}
	if err := c.setupChanels(); err != nil {
		return err
	}
	if err := c.setupQueues(); err != nil {
		return err
	}
	if err := c.reconsume(); err != nil {
		return err
	}
	return nil
}

func (c *Consumer) setupChanels() error {
	var err error
	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}
	c.logger.Debug("Success setup chanels to rabbitmq")
	return nil
}

func (c *Consumer) setupExchanges() error {
	for _, exchange := range c.exchanges {
		if err := exchange.declareAndBind(c.channel); err != nil {
			return err
		}
	}
	c.logger.Debug("Success setup exchanges in rabbitmq")
	return nil
}

func (c *Consumer) setupQueues() error {
	for _, queue := range c.queues {
		if err := queue.declare(c.channel); err != nil {
			return err
		}
	}
	c.logger.Debug("Success setup queues in rabbitmq")
	return nil
}

func (c *Consumer) consume() error {
	c.logger.Debug("Start consume queues")
	for _, queue := range c.queues {
		if queue.handler == nil {
			continue
		}

		if err := queue.consume(c.channel); err != nil {
			return err
		}
		for i := 0; i < queue.countWorkers; i++ {
			go c.consumeHandler(queue, i)
		}
	}

	// watcher for reconnect
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				c.logger.Debugf("Stopped watcher for reconnect")
				return
			case err := <-c.err:
				if err != nil {
					if err := c.reconnect(); err != nil {
						c.logger.Errorf("Failed reconnect to rabbitmq: %s", err)
					}
				}
			}

		}
	}()
	return nil
}

func (c *Consumer) reconsume() error {
	c.logger.Debug("Start reconsume queues")
	for _, queue := range c.queues {
		if queue.handler == nil {
			continue
		}

		if err := queue.consume(c.channel); err != nil {
			return fmt.Errorf("Failed reconsume queue=%s after reconnect: %s", queue.Name, err)
		}
		c.logger.Debugf("Success reconsume queue=%s after reconnect", queue.Name)
	}
	return nil
}

func (c *Consumer) consumeHandler(queue *Queue, workerNumber int) {
	c.logger.Debugf("Start process events: queue=%s, worker=%d", queue.Name, workerNumber)
	for {
		select {
		case delivery := <-queue.deliveries:
			c.logger.Debugf("Got event: queue=%s, worker=%d", queue.Name, workerNumber)
			if queue.handler(c.ctx, delivery) {
				if err := delivery.Ack(false); err != nil {
					c.logger.Errorf("Falied ack %s", queue.Name)
				}
				c.logger.Debugf("Ack event: queue=%s, worker=%d", queue.Name, workerNumber)
			} else {
				if err := delivery.Nack(false, queue.requeue); err != nil {
					c.logger.Errorf("Falied nack %s", queue.Name)
				}
				c.logger.Debugf("Nack event: queue=%s, requeue=%v, worker=%d", queue.Name, queue.requeue, workerNumber)
			}
		case <-c.ctx.Done():
			c.logger.Debugf("Stop process events: queue=%s, worker=%d", queue.Name, workerNumber)
			return
		}
	}
}
