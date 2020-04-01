package connector

import (
	"github.com/nats-io/nats.go"
)

type NatsConnector struct {
	NatsConnection *nats.Conn
	NatsSubscription *nats.Subscription
	QueueGroup string
	ServerUrls string
	SubscriptionChannel chan *nats.Msg
	Subject string
}

type SubscriberFunction func(*nats.Msg)

func(c *NatsConnector) Connect() error {
	nc, err := nats.Connect(c.ServerUrls)
	if (err != nil) {
		return err
	}
	c.NatsConnection = nc
	return nil
}

func(c *NatsConnector) QueueSubscribe(fn SubscriberFunction) error {
	subscription, err := c.NatsConnection.QueueSubscribe(c.Subject, c.QueueGroup, func(m *nats.Msg){ 
		go fn(m)
	})
	c.NatsSubscription = subscription
	return err
}

func (c *NatsConnector) Close() {
	_ = c.NatsConnection.Drain()
	c.NatsConnection.Close()
	close(c.SubscriptionChannel)
}