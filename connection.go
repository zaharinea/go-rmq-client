package rmqclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

const reconnectTimeout = 3 * time.Second

// Connection struct
type Connection struct {
	uri              string
	conn             *amqp.Connection
	channel          *amqp.Channel
	err              chan error
	ctx              context.Context
	notifyQuit       context.CancelFunc
	reconnectTimeout time.Duration
	logger           Logger
}

func (c *Connection) connect() error {
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

func (c *Connection) setupChanels() error {
	var err error
	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %w", err)
	}
	c.logger.Debug("Success setup chanels to rabbitmq")
	return nil
}

//Close close connection
func (c *Connection) Close() error {
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
