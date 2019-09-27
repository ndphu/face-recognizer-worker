package worker

import (
	"encoding/json"
	"face-recognizer-worker/config"
	"github.com/Kagami/go-face"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/ndphu/swd-commons/model"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

type Worker struct {
	Recognizer *face.Recognizer
	Categories []string
	Lock       sync.Mutex
	ProjectId  string
	stopC      chan bool
}

func NewWorker(projectId string) *Worker {
	rec, err := face.NewRecognizer("data")
	if err != nil {
		log.Fatalln("[RECOGNIZE]", "Fail to initialize recognizer by error:", err)
	}

	var newWorker = &Worker{
		ProjectId:  projectId,
		Lock:       sync.Mutex{},
		Categories: make([]string, 0),
		Recognizer: rec,
		stopC:      make(chan bool),
	}

	return newWorker
}

func (worker *Worker) Run() {
	broker := config.Config().RemoteSettings.MQTTBroker
	mqttClientId := uuid.New().String()

	log.Println("[MQTT]", "Connecting to MQTT", broker, "with client ID:", mqttClientId)
	opts := mqtt.NewClientOptions().AddBroker(broker).SetClientID(mqttClientId)
	opts.SetKeepAlive(2 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.SetConnectTimeout(30 * time.Second)

	opts.OnConnect = func(c mqtt.Client) {
		log.Println("[MQTT]", "Connected to MQTT")
		c.Subscribe("/3ml/worker/" + worker.ProjectId +"/rpc/recognizeFacesBulk/request", 0, func(c mqtt.Client, m mqtt.Message) {
			req := model.BulkRecognizeRequest{}
			if err := json.Unmarshal(m.Payload(), &req); err != nil {
				log.Println("[RPC] Error parsing request", len(m.Payload()))
				return
			}
			go worker.handleBulkRecognizeRequest(c, req)
		}).Wait()

		log.Println("[MQTT]", "Subscribed")
	}

	mqttClient := mqtt.NewClient(opts)

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalln("[MQTT]", "Fail to connect to MQTT Broker", token.Error().Error())
	}
	defer mqttClient.Disconnect(100)

	worker.reloadSamples()
	interval, err := time.ParseDuration(config.Config().RemoteSettings.SyncInterval)
	if err != nil {
		log.Fatalln("[SYNC]", "Fail to parse sync interval", config.Config().RemoteSettings.SyncInterval)
	}
	syncTicker := time.Tick(interval)
	for {
		select {
		case <-syncTicker:
			worker.reloadSamples()
			break
		case <-worker.stopC:
			log.Println("[Worker]", "Stopping worker of project", worker.ProjectId)
			worker.stopC <- true
			return
		}
	}

}

func (worker *Worker) Stop() {
	log.Println("[Worker]", "Requesting worker stop for project", worker.ProjectId)
	worker.stopC <- true
	<-worker.stopC
	log.Println("[Worker]", "Worker stopped for project", worker.ProjectId)
}

func (worker *Worker) handleBulkRecognizeRequest(client mqtt.Client, req model.BulkRecognizeRequest) {
	reqId := req.RequestId
	log.Println("[RPC]", reqId, "Request received")
	log.Println("[RPC]", reqId, "Number of images:", len(req.Images))

	labelSet := make(map[string]bool, 0)
	lock := sync.Mutex{}
	wg := sync.WaitGroup{}

	faceDetailsList := make([][]model.FaceDetails, len(req.Images))

	for i, f := range req.Images {
		wg.Add(1)
		go func(frame []byte, idx int) {
			defer wg.Done()
			if faces, err := worker.Recognizer.Recognize(frame); err != nil {
				log.Println("[RPC]", reqId, "Error recognize image")
			} else {
				if faces != nil {
					worker.Lock.Lock()
					faceDetailsList[idx] = make([]model.FaceDetails, 0)
					if len(faces) > 0 {
						log.Println("[RPC]", reqId, "Set recognized image index:", idx)
						for fdix, f := range faces {
							classified := worker.Recognizer.ClassifyThreshold(f.Descriptor, 0.15)
							if classified >= 0 && classified < len(worker.Categories) {
								lock.Lock()
								labelSet[worker.Categories[int32(classified)]] = true
								lock.Unlock()
							}
							faceDetailsList[idx] = append(faceDetailsList[idx], model.FaceDetails{
								Rect: faces[fdix].Rectangle,
								Descriptor: faces[fdix].Descriptor,
							})
						}
					}

					worker.Lock.Unlock()
				}
			}
		}(f, i)
	}

	wg.Wait()

	labels := make([]string, 0)

	for k := range labelSet {
		labels = append(labels, k)
	}

	rpcResp := model.BulkRecognizeResponse{
		Error:  nil,
		Labels: labels,
	}

	if req.IncludeFacesDetails {
		rpcResp.FaceDetailsList = faceDetailsList
	}

	payload, _ := json.Marshal(rpcResp)

	log.Println("[RPC]", reqId, "Sending response to", req.ResponseTo)
	client.Publish(req.ResponseTo, 0, false, payload).Wait()
	log.Println("[RPC]", reqId, "Response published successfully", req.ResponseTo)
}

func (worker *Worker) reloadSamples() error {
	log.Println("[WORKER]", "Reloading samples...")
	worker.Lock.Lock()
	defer worker.Lock.Unlock()

	if fis, err := worker.getFaces(); err != nil {
		log.Println("[WORKER]", "Fail to reload sample:", err.Error())
		return err
	} else {
		var descriptors []face.Descriptor
		var catIndexes []int32
		var categories []string
		for idx, faceInfo := range fis {
			descriptors = append(descriptors, faceInfo.Descriptor)
			catIndexes = append(catIndexes, int32(idx))
			categories = append(categories, faceInfo.Label)
		}
		worker.Recognizer.SetSamples(descriptors, catIndexes)
		worker.Categories = categories

		if jsonData, err := json.Marshal(fis); err != nil {
			log.Println("[WORKER]", "Fail to marshall data to write to file")
			return nil
		} else {
			if err := ioutil.WriteFile("faces.json", jsonData, 0644); err != nil {
				log.Println("[WORKER]", "Fail to write faces info to data file")
				return err
			} else {
			}
		}
		log.Println("[WORKER]", "Sample reload successfully")
		return nil
	}
}

func (worker *Worker) getFaces() ([]model.Face, error) {
	reloadUrl := config.Config().BackendBaseUrl + "/project/" + worker.ProjectId + "/faceInfos" + "?token=" + config.Config().Token
	log.Println("[WORKER]", "Reload URL:", reloadUrl)
	if resp, err := http.Get(reloadUrl); err != nil {
		return nil, err
	} else {
		defer resp.Body.Close()
		fis := make([]model.Face, 0)
		if body, err := ioutil.ReadAll(resp.Body); err != nil {
			return nil, err
		} else {
			err = json.Unmarshal(body, &fis)
			return fis, err
		}
	}
}
