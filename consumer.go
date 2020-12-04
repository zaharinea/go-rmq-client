package rmqclient

import (
	"context"
	"fmt"
	"sync"
)

// Consumer struct
type Consumer struct {
	Connection
	queues    map[string]*Queue
	exchanges map[string]*Exchange
	wg        *sync.WaitGroup
}

// NewConsumer returns a new Consumer struct
func NewConsumer(uri string, logger Logger) *Consumer {
	exchanges := make(map[string]*Exchange)
	queues := make(map[string]*Queue)
	err := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	return &Consumer{
		exchanges: exchanges,
		queues:    queues,
		Connection: Connection{
			uri:              uri,
			err:              err,
			ctx:              ctx,
			notifyQuit:       cancel,
			reconnectTimeout: reconnectTimeout,
			logger:           logger,
		},
		wg: wg,
	}
}

//Start start Consumer
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

//Stop stop Consumer
func (c *Consumer) Stop() error {
	c.notifyQuit()
	c.wg.Wait()
	return c.Close()
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
			c.wg.Add(1)
			go c.consumeWorker(queue, i)
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

func (c *Consumer) consumeWorker(queue *Queue, workerNumber int) {
	defer c.wg.Done()

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
