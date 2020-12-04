package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
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

	count := 0
	go func() {
		for {
			message := strconv.Itoa(count)
			fmt.Printf("Send message: %s\n", message)
			producer.Publish("exchange1", "queue1", []byte(message), 0)
			time.Sleep(time.Second * 1)
			count++
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	defer producer.Stop()
}
