package main

import (
	"encoding/base64"
	"encoding/json"
	"face-recognizer-worker/recognize"
	"github.com/Kagami/go-face"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"image"
	"log"
	"os"
	"os/signal"
	"time"
)

var recWrapper = recognize.RecognizerWrapper{}

func main() {
	rec, err := face.NewRecognizer("data")
	if err != nil {
		log.Fatalf("fail to initialize recognizer. %v\n", err)
	}
	defer rec.Close()

	recWrapper.Recognizer = rec

	broker := os.Getenv("MQTT_BROKER")
	if broker == "" {
		broker = "tcp://35.197.155.112:4443"
	}
	clientId := uuid.New().String()
	log.Println("Connecting to MQTT", broker, "with client ID:", clientId)
	opts := mqtt.NewClientOptions().AddBroker(broker).SetClientID(clientId)
	opts.SetKeepAlive(2 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.SetConnectTimeout(30 * time.Second)

	opts.OnConnect = func(c mqtt.Client) {
		log.Println("Connected to MQTT")
		c.Subscribe("/3ml/rpc/recognizeFaces/request", 0, func(c mqtt.Client, message mqtt.Message) {
			req := RecognizeRequest{}
			if err := json.Unmarshal(message.Payload(), &req); err != nil {
				log.Println("[RPC] error parsing request", string(message.Payload()))
				return
			}
			reqId := req.RequestId
			log.Println("[RPC]", reqId, "request received")
			frame, err := base64.StdEncoding.DecodeString(req.Payload)
			if err != nil {
				log.Println("[RPC]", reqId, "error recoding base64 image")
				return
			}
			if faces, err := recWrapper.Recognizer.Recognize(frame); err != nil {
				log.Println("[RPC]", reqId, "error recognize image")
			} else {
				var rfs []RecognizedFace
				if faces != nil {
					for _, f := range faces {
						classified := recWrapper.Recognizer.ClassifyThreshold(f.Descriptor, 0.15)
						if classified >= 0 {
							rfs = append(rfs, RecognizedFace{
								Rect:       f.Rectangle,
								Label:      recWrapper.Categories[int32(classified)],
								Classified: classified,
								Descriptor: f.Descriptor,
							})
						} else {
							rfs = append(rfs, RecognizedFace{
								Rect:       f.Rectangle,
								Label:      "UNKNOWN",
								Classified: classified,
								Descriptor: f.Descriptor,
							})
						}
					}
				}
				rpcResp := RecognizedResponse{
					Error:           nil,
					RecognizedFaces: rfs,
				}
				payload, _ := json.Marshal(rpcResp)
				respTopic := "/3ml/rpc/recognizeFaces/response/" + reqId
				log.Println("[RPC]", reqId, "sending response to", respTopic)
				c.Publish(respTopic, 0, false, payload).Wait()
				log.Println("[RPC]", reqId, "response published successfully", respTopic)
			}
		}).Wait()
		log.Println("Subscribed")
	}

	mqttClient := mqtt.NewClient(opts)

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error().Error())
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	log.Println("Interrupt signal received. Exiting...")
	os.Exit(0)

}

type RecognizedFace struct {
	Rect       image.Rectangle `json:"rect"`
	Label      string          `json:"label"`
	Classified int             `json:"category"`
	Descriptor [128]float32    `json:"descriptor"`
}

type DetectedFace struct {
	Rect       image.Rectangle `json:"rect"`
	Descriptor face.Descriptor `json:"descriptor"`
}

type RecognizeRequest struct {
	Payload   string `json:"payload"`
	RequestId string `json:"requestId"`
}
type RecognizedResponse struct {
	RecognizedFaces []RecognizedFace `json:"recognizedFaces"`
	Error           error            `json:"error"`
}
