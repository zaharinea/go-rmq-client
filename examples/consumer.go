package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	rmqclient "github.com/zaharinea/go-rmq-client"
)

func handler(ctx context.Context, msg amqp.Delivery) bool {
	fmt.Printf("event: msg=%s", string(msg.Body))
	return true
}

func main() {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	consumer := rmqclient.NewConsumer("amqp://guest:guest@rmq:5672/", logger)

	queue1 := rmqclient.NewQueue("queue1", "queue1", amqp.Table{}).SetHandler(handler).SetRequeue(false).SetCountWorkers(4)
	companyExchange := rmqclient.NewExchange("exchange1", "fanout", amqp.Table{}, []*rmqclient.Queue{queue1})
	consumer.RegisterExchange(companyExchange)

	queue2Failed := rmqclient.NewQueue("queue2-failed", "", amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": "queue2",
		"x-message-ttl":             60 * 1000,
	}).SetRequeue(false)
	queue2 := rmqclient.NewQueue("queue2", "", amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": "queue2-failed",
	}).SetRequeue(false).SetHandler(handler).SetCountWorkers(4)
	consumer.RegisterQueue(queue2, queue2Failed)

	consumer.Start()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	defer consumer.Close()
}