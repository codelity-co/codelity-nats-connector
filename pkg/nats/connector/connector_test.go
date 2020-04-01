package connector_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	connectorPkg "bitbucket.org/codelity-co/codelity-nats-connector/pkg/nats/connector"

	nats "github.com/nats-io/nats.go"
	natstest "github.com/nats-io/nats-server/test"
	"github.com/nats-io/gnatsd/server"
)

var (
	_ = Describe("Nats", func() {

		var (
			s *server.Server
		)

		BeforeSuite(func() {
			s = natstest.RunDefaultServer()
		})

	  AfterSuite(func() {
			s.Shutdown()
		})
		
		Describe("Connection Test", func() {
			It("Simple PubSub Test", func() {

				nc, err := nats.Connect("nats://localhost:4222")
				Expect(err).To(BeNil(), "err = %v", err)
				
				publishChan := make(chan *nats.Msg)
				subscribeChan := make(chan *nats.Msg)

				nc.Subscribe("test.pubsub", func(m *nats.Msg){ //nolint:errcheck
					fmt.Println("received message")
					subscribeChan <- m
				})

				go func(){
					testMessage := <- publishChan
					nc.PublishMsg(testMessage) //nolint:errcheck
				}()

				testMessage := &nats.Msg{ 
					Subject: "test.pubsub",
					Data: []byte("test"),
				}

				publishChan <- testMessage
				message := <- subscribeChan

				Expect(message.Subject).To(Equal("test.pubsub"), "Subject = %v", message.Subject)
				Expect(string(message.Data)).To(Equal("test"), "Data = %v", string(message.Data))

			})

			It("Simple Queue Test", func() {

				nc, err := nats.Connect("nats://localhost:4222")
				Expect(err).To(BeNil(), "err = %v", err)
				
				publishChan := make(chan *nats.Msg)
				subscribeChan := make(chan *nats.Msg)

				nc.QueueSubscribe("test.queue", "testQueue", func(m *nats.Msg){ //nolint:errcheck
					fmt.Println("received message")
					subscribeChan <- m
				})

				go func(){
					testMessage := <- publishChan
					nc.PublishMsg(testMessage) //nolint:errcheck
				}()

				testMessage := &nats.Msg{ 
					Subject: "test.queue",
					Data: []byte("test"),
				}

				publishChan <- testMessage
				message := <- subscribeChan

				Expect(message.Subject).To(Equal("test.queue"), "Subject = %v", message.Subject)
				Expect(string(message.Data)).To(Equal("test"), "Data = %v", string(message.Data))

			})

		})

		Describe("NatsConnector Connect", func() {

			It("Connection Positive Test", func() {
				connector := &connectorPkg.NatsConnector{
					ServerUrls: "nats://localhost:4222",
				}
				Expect(connector.NatsConnection).To(BeNil())

				err1 := connector.Connect()

				Expect(err1).To(BeNil(), "err1 = %v", err1)
				Expect(connector.NatsConnection).NotTo(BeNil())
			})

			It("Connection Negative Test", func() {
				connector := &connectorPkg.NatsConnector{
					ServerUrls: "nats://localhost:4221",
				}
				Expect(connector.NatsConnection).To(BeNil())

				err1 := connector.Connect()
				Expect(err1).NotTo(BeNil(), "err1 = %v", err1)
				Expect(connector.NatsConnection).To(BeNil())
			})

		})

		Describe("NatsConnector QueueSubscribe", func() {

			It("Connection Positive Test", func() {
				subscriptionChannel := make(chan *nats.Msg)
				connector := &connectorPkg.NatsConnector{
					QueueGroup: "queueGroup",
					ServerUrls: "nats://localhost:4222",
					Subject: "test.queue",
					SubscriptionChannel: subscriptionChannel,
				}
				Expect(connector.NatsConnection).To(BeNil())

				err1 := connector.Connect()

				Expect(err1).To(BeNil(), "err1 = %v", err1)
				Expect(connector.NatsConnection).NotTo(BeNil())

				err2 := connector.QueueSubscribe(func(m *nats.Msg){
					connector.SubscriptionChannel <- m
					fmt.Println(fmt.Sprintf("Received message = %v", m))
				})
				Expect(err2).To(BeNil(), "err2 = %v", err2)
				Expect(connector.NatsSubscription).NotTo(BeNil(), "subscription = %v", connector.NatsSubscription)

				testMessage := &nats.Msg{
					Subject: "test.queue", 
					Data: []byte("test"),
				}
				err := connector.NatsConnection.PublishMsg(testMessage)
				Expect(err).To(BeNil())

				message := <- connector.SubscriptionChannel

				Expect(message.Subject).To(Equal("test.queue"), "Subject = %v", message.Subject)
				Expect(string(message.Data)).To(Equal("test"), "Data = %v", string(message.Data))
			})

			It("Connection Negative Test", func() {
				subscriptionChannel := make(chan *nats.Msg)
				connector := &connectorPkg.NatsConnector{
					QueueGroup: "queueGroup",
					ServerUrls: "nats://localhost:4222",
					Subject: "test.queue1",
					SubscriptionChannel: subscriptionChannel,
				}
				Expect(connector.NatsConnection).To(BeNil())

				err1 := connector.Connect()

				Expect(err1).To(BeNil(), "err1 = %v", err1)
				Expect(connector.NatsConnection).NotTo(BeNil())

				err2 := connector.QueueSubscribe(func(m *nats.Msg){
					connector.SubscriptionChannel <- m
					fmt.Println(fmt.Sprintf("Received message = %v", m))
				})
				Expect(err2).To(BeNil(), "err2 = %v", err2)
				Expect(connector.NatsSubscription).NotTo(BeNil(), "subscription = %v", connector.NatsSubscription)

				testMessage := &nats.Msg{
					Subject: "test.queue", 
					Data: []byte("test"),
				}
				err3 := connector.NatsConnection.PublishMsg(testMessage)
				Expect(err3).To(BeNil(), "Error = %v", err3)

				ticker := time.NewTicker(time.Duration(time.Second * 2))
				go func() {
					<- ticker.C
					connector.NatsSubscription.Unsubscribe() //nolint:errcheck
					connector.SubscriptionChannel <- nil
				}()
				
				message := <- connector.SubscriptionChannel

				Expect(message).To(BeNil(), "Message = %v", message)
			})

		})

	})
)

