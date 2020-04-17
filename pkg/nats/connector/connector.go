package connector

import (
	"fmt"
	"time"

	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
)

type NatsConnector struct {
	AllowReconnect bool
	EnableStreaming bool
	QueueGroup string
	NatsConnection *nats.Conn
	NatsSubscription *nats.Subscription
	ReconnectMaxRetry int
	ReconnectWaitTime time.Duration
	ServerUrls string
	SubscriptionChannel chan map[string]interface{}
	StanConnection *stan.Conn
	StanClusterId string
	StanChannelId string
	StanSubscription *stan.Subscription
	Subject string
}

type SubscriberFunction func(map[string]interface{})

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
	if c.EnableStreaming && c.StanConnection == nil {
		if len(c.StanClusterId) == 0 {
			return fmt.Errorf("StanClusterId cannot be empty")
		}
		if len(c.StanChannelId) == 0 {
			return fmt.Errorf("StanChannelId cannot be empty")
		}
		sc, err := stan.Connect(c.StanClusterId, c.StanChannelId, stan.NatsConn(c.NatsConnection))
		if err != nil {
			return err
		}
		c.StanConnection = &sc
		return nil
	}

	var err error 

	if c.EnableStreaming {

		var sc stan.Conn = *c.StanConnection
		var subsc stan.Subscription 
		subsc, err = sc.QueueSubscribe(c.Subject, c.QueueGroup, func(m *stan.Msg){
			payload := map[string]interface{}{
				"stanMsg": m,
			}
			go fn(payload)
		})
		if err != nil {
			return err
		}
		c.StanSubscription = &subsc
	} else {
		c.NatsSubscription, err = c.NatsConnection.QueueSubscribe(c.Subject, c.QueueGroup, func(m *nats.Msg){ 
			payload := map[string]interface{}{
				"natsMsg": m,
			}
			go fn(payload)
		})
	}

	return err
}

func (c *NatsConnector) Close() {
	_ = c.NatsConnection.Drain()
	c.NatsConnection.Close()
	close(c.SubscriptionChannel)
}