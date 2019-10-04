package worker

import (
	"encoding/json"
	"face-recognizer-worker/config"
	"github.com/Kagami/go-face"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/ndphu/swd-commons/model"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

type Worker struct {
	Recognizer *face.Recognizer
	Categories []string
	Lock       sync.Mutex
	DeskId     string
	stopC      chan bool
}

func NewWorker(deskId string) *Worker {
	rec, err := face.NewRecognizer("data")
	if err != nil {
		log.Fatalln("[RECOGNIZE]", "Fail to initialize recognizer by error:", err)
	}

	var newWorker = &Worker{
		DeskId:     deskId,
		Lock:       sync.Mutex{},
		Categories: make([]string, 0),
		Recognizer: rec,
		stopC:      make(chan bool),
	}

	return newWorker
}

func (worker *Worker) HandleRecognizeRequest(client mqtt.Client, req model.RecognizeRequest) {
	defer worker.Close()

	reqId := req.RequestId
	log.Println("[RPC]", reqId, "Number of images:", len(req.Images), "IncludeFaceDetails:", req.IncludeFacesDetails, "Should ClassifyFaces:", req.ClassifyFaces)

	labelSet := make(map[string]bool, 0)
	lock := sync.Mutex{}
	wg := sync.WaitGroup{}

	if len(req.FacesData) > 0 {
		// The request already provide face data, just use this data for recognition
		log.Println("[RPC]", reqId, "Request provided faces data:", len(req.FacesData))
		worker.reloadSamples(req.FacesData)
	} else {
		if req.ClassifyFaces {
			if err := worker.reloadSamplesFromServer(req.AccessToken); err != nil {
				log.Println("[RPC]", reqId, "Fail to reload samples", len(req.Images))
				return
			}
		}
	}
	faceDetailsList := make([][]model.FaceDetails, len(req.Images))
	for i, f := range req.Images {
		wg.Add(1)
		go func(frame []byte, idx int) {
			defer wg.Done()
			if faces, err := worker.Recognizer.Recognize(frame); err != nil {
				log.Println("[RPC]", reqId, "Error recognize image")
			} else {
				if faces != nil {
					faceDetailsList[idx] = make([]model.FaceDetails, 0)
					if len(faces) > 0 {
						log.Println("[RPC]", reqId, "Set recognized image index:", idx)
						for faceIndex, f := range faces {
							if req.ClassifyFaces {
								classified := worker.Recognizer.ClassifyThreshold(f.Descriptor, 0.15)
								if classified >= 0 && classified < len(worker.Categories) {
									lock.Lock()
									labelSet[worker.Categories[int32(classified)]] = true
									lock.Unlock()
								}
							}
							if req.IncludeFacesDetails {
								faceDetailsList[idx] = append(faceDetailsList[idx], model.FaceDetails{
									Rect:       faces[faceIndex].Rectangle,
									Descriptor: faces[faceIndex].Descriptor,
								})
							}
						}
					}
				}
			}
		}(f, i)
	}

	wg.Wait()

	labels := make([]string, 0)
	for k := range labelSet {
		labels = append(labels, k)
	}
	rpcResp := model.RecognizeResponse{
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

func (worker *Worker) reloadSamples(faces []model.Face) error {
	log.Println("[WORKER]", "Reloading samples...")
	worker.Lock.Lock()
	defer worker.Lock.Unlock()

	var descriptors []face.Descriptor
	var catIndexes []int32
	var categories []string
	for idx, faceInfo := range faces {
		descriptors = append(descriptors, faceInfo.Descriptor)
		catIndexes = append(catIndexes, int32(idx))
		categories = append(categories, faceInfo.UserId.Hex())
	}
	worker.Recognizer.SetSamples(descriptors, catIndexes)
	worker.Categories = categories
	return nil
}

func (worker *Worker) reloadSamplesFromServer(accessToken string) error {
	log.Println("[WORKER]", "Reloading samples from web server...")
	worker.Lock.Lock()
	defer worker.Lock.Unlock()

	if fis, err := worker.getFaces(accessToken); err != nil {
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

func (worker *Worker) getFaces(accessToken string) ([]model.Face, error) {
	reloadUrl := config.Config().BackendBaseUrl + "/desk/" + worker.DeskId + "/faceInfos" + "?accessToken=" + accessToken
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
func (worker *Worker) Close() {
	worker.Recognizer.Close()
}
