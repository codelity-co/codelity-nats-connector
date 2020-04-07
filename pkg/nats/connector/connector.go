package connector

import (
	"time"

	"github.com/nats-io/nats.go"
)

type NatsConnector struct {
	AllowReconnect bool
	NatsConnection *nats.Conn
	NatsSubscription *nats.Subscription
	QueueGroup string
	ReconnectMaxRetry int
	ReconnectWaitTime time.Duration
	ServerUrls string
	SubscriptionChannel chan *nats.Msg
	Subject string
}

type SubscriberFunction func(*nats.Msg)

func(c *NatsConnector) Connect() error {
	opts := nats.GetDefaultOptions()
	opts.Url = c.ServerUrls
	opts.AllowReconnect = c.AllowReconnect
	opts.ReconnectWait = c.ReconnectWaitTime
	opts.MaxReconnect = c.ReconnectMaxRetry
	nc, err := opts.Connect()
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