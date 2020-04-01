# Codelity NATS Connector

A Go connector for the NATS messaging system.

## Basic Usage

```
import (
  "fmt"
  "bitbucket.org/codelity-co/codelity-nats-connector/pkg/nats/connector"
)

subscriptionChannel := make(chan *nats.Msg)
connector := &connectorPkg.NatsConnector{
  QueueGroup: "queueGroup",
  ServerUrls: "nats://localhost:4222",
  Subject: "test.queue1",
  SubscriptionChannel: subscriptionChannel,
}

if err1 := connector.Connect(); err != nil {
  fmt.Println(err1)
}

if err2 := connector.QueueSubscribe(func(m *nats.Msg){
  connector.SubscriptionChannel <- m
}); err2 != nil {
  fmt.Println(err2)
}

testMessage := &nats.Msg{
  Subject: "test.queue", 
  Data: []byte("test"),
}
err := connector.NatsConnection.PublishMsg(testMessage)

message := <- connector.SubscriptionChannel
fmt.Println(message)

```