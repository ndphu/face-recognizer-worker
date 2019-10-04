package main

import (
	"encoding/json"
	"face-recognizer-worker/config"
	"face-recognizer-worker/worker"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/ndphu/swd-commons/model"
	"github.com/ndphu/swd-commons/service"
	"log"
	"os"
	"os/signal"
)

func main() {
	topic := "/3ml/recognize/request"

	opts := service.NewClientOpts(config.Config().RemoteSettings.MQTTBroker)
	opts.OnConnect = func(client mqtt.Client) {
		log.Println("[MQTT]", "Connected to broker")
		client.Subscribe(topic, 0, func(client mqtt.Client, message mqtt.Message) {
			req := model.RecognizeRequest{}
			if err := json.Unmarshal(message.Payload(), &req); err != nil {
				log.Println("[MQTT]", "Fail to unmarshal message")
				return
			}
			log.Println("[MQTT]", "RecognizeRequest received")
			w := worker.NewWorker(req.DeskId)
			go w.HandleRecognizeRequest(client, req)
		}).Wait()
		log.Println("[MQTT]", "Subscribed to BulkRecognizeRequest topic", topic)
	}
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); tok.Wait() && tok.Error() != nil {
		log.Panic("[MQTT]", "Fail to connect to message broker", tok.Error())
	}

	defer c.Disconnect(100)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	log.Println("Interrupt signal received. Exiting...")
	os.Exit(0)
}