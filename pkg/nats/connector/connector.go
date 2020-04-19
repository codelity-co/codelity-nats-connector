package connector

import (
	"fmt"
	"os"
	"time"

	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
	"github.com/sirupsen/logrus"
)

var (
	err error
	nc *nats.Conn
	sc stan.Conn
	hostname string
	stanSubscription stan.Subscription
)

type NatsConnector struct {
	AllowReconnect      bool
	EnableStreaming     bool
	QueueGroup          string
	NatsConnection      *nats.Conn
	NatsSubscription    *nats.Subscription
	ReconnectMaxRetry   int
	ReconnectWaitTime   time.Duration
	ServerUrls          string
	SubscriptionChannel chan map[string]interface{}
	StanConnection      *stan.Conn
	StanClusterId       string
	StanChannelId       string
	StanDurableName		  string
	StanSubscription    *stan.Subscription
	Subject             string
}

type SubscriberFunction func(*logrus.Logger, map[string]interface{})

func (c *NatsConnector) Connect(logger *logrus.Logger) error {
	opts := nats.GetDefaultOptions()
	opts.Url = c.ServerUrls
	opts.AllowReconnect = c.AllowReconnect
	opts.ReconnectWait = c.ReconnectWaitTime
	opts.MaxReconnect = c.ReconnectMaxRetry
	if logger != nil {
		logger.Debug(fmt.Sprintf("opts = %v", opts))
	}
	if c.NatsConnection == nil {
		nc, err = opts.Connect()
		if err != nil {
			return err
		}
		c.NatsConnection = nc
	}

	if c.EnableStreaming {
		if c.StanConnection == nil {
			if len(c.StanClusterId) == 0 {
				return fmt.Errorf("StanClusterId cannot be empty")
			}
			if len(c.StanChannelId) == 0 {
				return fmt.Errorf("StanChannelId cannot be empty")
			}
			hostname, err = os.Hostname()
			if err != nil {
				return err
			}
			if logger != nil {
				logger.Debug(fmt.Sprintf("c.StanClusterId = %v", c.StanClusterId))
				logger.Debug(fmt.Sprintf("c.StanChannelId = %v", c.StanChannelId))
				logger.Debug(fmt.Sprintf("hostname = %v", hostname))
				logger.Debug("Connecting to NATS Streaming Server")
			}
			sc, err = stan.Connect(c.StanClusterId, hostname, stan.NatsConn(nc))
			if err != nil {
				return err
			}
			c.StanConnection = &sc
			if logger != nil {
				logger.Debug(fmt.Sprintf("c.StanConnection = %v", c.StanConnection))
				logger.Debug("Connected NATS Streaming Serversuccessfully")
			}
			
		}
	}

	return nil
}

func (c *NatsConnector) QueueSubscribe(logger *logrus.Logger, fn SubscriberFunction) error {

	if logger != nil {
		logger.Debug("Running NatsConnector.QueueSubscribe...")
		logger.Debug(fmt.Sprintf("natsConnector.EnableStreaming = %v", c.EnableStreaming))
		logger.Debug(fmt.Sprintf("natsConnector.StanConnection = %v", c.StanConnection))	
	}

	if c.EnableStreaming && c.StanConnection == nil {
		if logger != nil {
			logger.Debug("Call natsConnector.Connect...")
		}
		err = c.Connect(logger)
		if err != nil {
			return err
		}
		if logger != nil {
			logger.Debug(fmt.Sprintf("natsConnector.StanConnection = %v", c.StanConnection))
		}
		
	}

	if c.EnableStreaming && c.StanConnection != nil {

		if len(c.StanChannelId) == 0 {
			return fmt.Errorf("StanChannelId cannot be empty")
		}
		if len(c.QueueGroup) == 0 {
			return fmt.Errorf("QueueGroup cannot be empty")
		}
		
		if logger != nil {
			logger.Debug("Calling StanConnection.QueueSubscribe...")
		}
		
		sc = *c.StanConnection
		if len(c.StanDurableName) > 0 {
			if logger != nil {
				logger.Debug(fmt.Sprintf("c.StanDurableName = %v", c.StanDurableName))
			}
			stanSubscription, err = sc.QueueSubscribe(c.StanChannelId, c.QueueGroup, func(m *stan.Msg) {
				if logger != nil {
					logger.Debug(fmt.Sprintf("stan.Msg = %v", *m))
				}
				payload := map[string]interface{}{
					"stanMsg": m,
				}
				go fn(logger, payload)
			}, stan.DurableName(c.StanDurableName))
		} else {
			stanSubscription, err = sc.QueueSubscribe(c.StanChannelId, c.QueueGroup, func(m *stan.Msg) {
				if logger != nil {
					logger.Debug(fmt.Sprintf("stan.Msg = %v", *m))
				}
				payload := map[string]interface{}{
					"stanMsg": m,
				}
				go fn(logger, payload)
			})
		}

		if err != nil {
			return err
		}
		c.StanSubscription = &stanSubscription

		if logger != nil {
			logger.Debug("Called StanConnection.QueueSubscribe successfully")
		}

	} else {

		if logger != nil {
			logger.Debug("Calling NatsConnection.QueueSubscribe...")
		}

		c.NatsSubscription, err = c.NatsConnection.QueueSubscribe(c.Subject, c.QueueGroup, func(m *nats.Msg) {
			if logger != nil {
				logger.Debug(fmt.Sprintf("nats.Msg = %v", *m))
			}
			payload := map[string]interface{}{
				"natsMsg": m,
			}
			go fn(logger, payload)
		})

		if err != nil {
			return err
		}

		if logger != nil {
			logger.Debug("Called NatsConnection.QueueSubscribe successfully")
		}

	}

	return err
}

func (c *NatsConnector) Close() {
	_ = c.NatsConnection.Drain()
	c.NatsConnection.Close()
	close(c.SubscriptionChannel)
}
