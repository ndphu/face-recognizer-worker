package main

import (
	"encoding/base64"
	"encoding/json"
	"face-recognizer-worker/config"
	"face-recognizer-worker/recognize"
	"github.com/Kagami/go-face"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"image"
	"log"
	"os"
	"os/signal"
	"sync"
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

	broker := config.Config().RemoteSettings.MQTTBroker
	mqttClientId := uuid.New().String()

	log.Println("Connecting to MQTT", broker, "with client ID:", mqttClientId)
	// TODO: implement authenticate MQTT
	opts := mqtt.NewClientOptions().AddBroker(broker).SetClientID(mqttClientId)
	opts.SetKeepAlive(2 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.SetConnectTimeout(30 * time.Second)

	opts.OnConnect = func(c mqtt.Client) {
		log.Println("Connected to MQTT")
		c.Subscribe("/3ml/rpc/recognizeFaces/request", 0, func(c mqtt.Client, m mqtt.Message) {
			req := RecognizeRequest{}
			if err := json.Unmarshal(m.Payload(), &req); err != nil {
				log.Println("[RPC] error parsing request", string(m.Payload()))
				return
			}
			go handleRecognizeRequest(c, req)
		}).Wait()
		c.Subscribe("/3ml/rpc/sync/request", 0, func(c mqtt.Client, m mqtt.Message) {
			recognize.ReloadSamples(&recWrapper)
		}).Wait()

		c.Subscribe("/3ml/rpc/recognizeFacesBulk/request", 0, func(c mqtt.Client, m mqtt.Message) {
			req := BulkRecognizeRequest{}
			if err := json.Unmarshal(m.Payload(), &req); err != nil {
				log.Println("[RPC] error parsing request", string(m.Payload()))
				return
			}
			go handleBulkRecognizeRequest(c, req)
		}).Wait()

		log.Println("Subscribed")
	}

	mqttClient := mqtt.NewClient(opts)

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error().Error())
	}
	interval, err := time.ParseDuration(config.Config().RemoteSettings.SyncInterval)
	if err != nil {
		log.Panic(err)
	}
	go func() {
		recognize.ReloadSamples(&recWrapper)
		for {
			<-time.Tick(interval)
			recognize.ReloadSamples(&recWrapper)
		}
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	log.Println("Interrupt signal received. Exiting...")
	os.Exit(0)

}

func handleRecognizeRequest(client mqtt.Client, req RecognizeRequest) {
	reqId := req.RequestId
	log.Println("[RPC]", reqId, "request received")
	log.Println("[RPC]", reqId, "payload", req.Payload)
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
			recWrapper.Lock.Lock()
			for _, f := range faces {
				classified := recWrapper.Recognizer.ClassifyThreshold(f.Descriptor, 0.15)
				log.Println("classified", classified)
				log.Println("recWrapper.Categories", recWrapper.Categories)
				if classified >= 0 && classified < len(recWrapper.Categories) {
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
			recWrapper.Lock.Unlock()
		}
		rpcResp := RecognizedResponse{
			Error:           nil,
			RecognizedFaces: rfs,
		}
		payload, _ := json.Marshal(rpcResp)
		respTopic := "/3ml/rpc/recognizeFaces/response/" + reqId
		log.Println("[RPC]", reqId, "sending response to", respTopic)
		client.Publish(respTopic, 0, false, payload).Wait()
		log.Println("[RPC]", reqId, "response published successfully", respTopic)
	}
}

func handleBulkRecognizeRequest(client mqtt.Client, req BulkRecognizeRequest) {
	reqId := req.RequestId
	log.Println("[RPC]", reqId, "request received")
	log.Println("[RPC]", reqId, "number of images:", len(req.Images))

	labelSet := make(map[string]bool, 0)
	lock := sync.Mutex{}
	wg := sync.WaitGroup{}

	for _, f := range req.Images {
		wg.Add(1)
		go func(frame []byte) {
			defer wg.Done()
			if faces, err := recWrapper.Recognizer.Recognize(frame); err != nil {
				log.Println("[RPC]", reqId, "error recognize image")
			} else {
				if faces != nil {
					recWrapper.Lock.Lock()
					for _, f := range faces {
						classified := recWrapper.Recognizer.ClassifyThreshold(f.Descriptor, 0.15)
						log.Println("classified", classified)
						log.Println("recWrapper.Categories", recWrapper.Categories)
						if classified >= 0 && classified < len(recWrapper.Categories) {
							lock.Lock()
							labelSet[recWrapper.Categories[int32(classified)]] = true
							lock.Unlock()
						}
					}
					recWrapper.Lock.Unlock()
				}
			}
		}(f)
	}

	wg.Wait()

	labels := make([]string, 0)

	for k := range labelSet {
		labels = append(labels, k)
	}

	rpcResp := BulkRecognizeResponse{
		Error:  nil,
		Labels: labels,
	}

	payload, _ := json.Marshal(rpcResp)
	respTopic := "/3ml/rpc/recognizeFacesBulk/response/" + reqId
	log.Println("[RPC]", reqId, "sending response to", respTopic)
	client.Publish(respTopic, 0, false, payload).Wait()
	log.Println("[RPC]", reqId, "response published successfully", respTopic)
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

type BulkRecognizeRequest struct {
	Images    [][]byte `json:"payload"`
	RequestId string   `json:"requestId"`
}

type BulkRecognizeResponse struct {
	Labels []string `json:"labels"`
	Error  error    `json:"error"`
}
