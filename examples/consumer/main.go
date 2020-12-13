package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	rmqclient "github.com/zaharinea/go-rmq-client"
)

func loggingMiddleware(handler rmqclient.HandlerFunc) rmqclient.HandlerFunc {
	return func(ctx context.Context, msg amqp.Delivery) bool {
		queueName := ctx.Value(rmqclient.QueueNameKey).(string)
		fmt.Printf("start processing event: queue=%s, msg=%s\n", queueName, string(msg.Body))
		res := handler(ctx, msg)
		fmt.Printf("end processing event: queue=%s, msg=%s\n", queueName, string(msg.Body))
		return res
	}
}

func handler(ctx context.Context, msg amqp.Delivery) bool {
	time.Sleep(time.Second * 3)
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
	consumer.RegisterMiddleware(loggingMiddleware)

	consumer.Start()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	defer consumer.Stop()
}
