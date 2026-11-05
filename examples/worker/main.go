package main

import (
	"log"
	"os"
	"time"

	"github.com/NARUBROWN/spine"
	natsplatform "github.com/NARUBROWN/spine-nats-platform"
	"github.com/NARUBROWN/spine/pkg/boot"
)

type OrderCreated struct {
	ID int `json:"id"`
}

type OrderConsumer struct{}

func (*OrderConsumer) Handle(input OrderCreated) {
	log.Printf("주문 수신: id=%d", input.ID)
}

func main() {
	factory, err := natsplatform.NewFactory(natsplatform.Config{
		URLs: []string{os.Getenv("NATS_URL")}, Stream: os.Getenv("NATS_STREAM"),
		Consume: true, ConsumerPrefix: "orders", Token: os.Getenv("NATS_TOKEN"),
		AllowInsecureTransport: os.Getenv("NATS_ALLOW_INSECURE") == "true",
		FailurePolicy:          natsplatform.FailureRequeue, RequeueDelay: time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	app := spine.New(spine.WithEventAdapter("nats", factory))
	app.Constructor(func() *OrderConsumer { return &OrderConsumer{} })
	if err := app.Consumers().Register("order.created", (*OrderConsumer).Handle); err != nil {
		log.Fatal(err)
	}
	if err := app.Run(boot.Options{EnableGracefulShutdown: true, ShutdownTimeout: 10 * time.Second}); err != nil {
		log.Fatal(err)
	}
}
