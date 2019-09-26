package recognize

import (
	"encoding/json"
	"face-recognizer-worker/config"
	"github.com/Kagami/go-face"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

type RecognizerWrapper struct {
	Recognizer *face.Recognizer
	Categories []string
	Lock       sync.Mutex
}

func ReloadSamples(wrapper *RecognizerWrapper) error {
	log.Println("Reloading samples...")
	wrapper.Lock.Lock()
	defer wrapper.Lock.Unlock()

	if fis, err := GetFaces(); err != nil {
		log.Println("Fail to reload sample:", err.Error())
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
		wrapper.Recognizer.SetSamples(descriptors, catIndexes)
		wrapper.Categories = categories

		if jsonData, err := json.Marshal(fis); err != nil {
			log.Println("fail to marshall data to write to file")
			return nil
		} else {
			if err := ioutil.WriteFile("faces.json", jsonData, 0644); err != nil {
				log.Println("fail to write faces info to data file")
				return err
			} else {
			}
		}
		log.Println("sample reload successfully")
		return nil
	}
}

func GetFaces() ([]FaceInfo, error) {
	reloadUrl := config.Config().BackendBaseUrl + "/project/" + config.Config().ProjectId + "/faceInfos" + "?token=" + config.Config().Token
	log.Println("Reload URL:", reloadUrl)
	if resp, err := http.Get(reloadUrl); err != nil {
		return nil, err
	} else {
		defer resp.Body.Close()
		fis := make([]FaceInfo, 0)
		if body, err := ioutil.ReadAll(resp.Body); err != nil {
			return nil, err
		} else {
			err = json.Unmarshal(body, &fis)
			return fis, err
		}
	}
}
