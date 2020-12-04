# go-rmq-client
Simple client for rabbitmq

## example consumer usage
```go
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
	fmt.Printf("event: msg=%s\n", string(msg.Body))
	return true
}

func main() {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	consumer := rmqclient.NewConsumer("amqp://guest:guest@rmq:5672/", logger)

	queue1 := rmqclient.NewQueue("queue1", "queue1", amqp.Table{}).SetHandler(handler).SetRequeue(false).SetCountWorkers(4)
	companyExchange := rmqclient.NewExchange("exchange1", "fanout", amqp.Table{}, []*rmqclient.Queue{queue1})
	consumer.RegisterExchange(companyExchange)

	consumer.Start()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	consumer.Stop()
}
```


## example producer usage
```go
package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	rmqclient "github.com/zaharinea/go-rmq-client"
)

func main() {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	producer := rmqclient.NewProducer("amqp://guest:guest@rmq:5672/", logger)

	producer.Start()

	var count uint64
	go func() {
		for {
			producer.Publish("exchange1", "queue1", []byte(string(count)), 0)
			time.Sleep(time.Second * 1)
			count++
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	defer producer.Stop()
}
```