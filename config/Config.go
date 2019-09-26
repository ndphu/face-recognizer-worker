package config

import (
	"encoding/json"
	"log"
	"os"
)

type AppConfig struct {
	ProjectId      string         `json:"projectId"`
	BackendBaseUrl string         `json:"backendBaseUrl"`
	Token          string         `json:"token"`
	RemoteSettings RemoteSettings `json:"remoteSettings"`
}

type RemoteSettings struct {
	MQTTBroker   string `json:"mqttBroker"`
	MQTTUsername string `json:"mqttUsername"`
	MQTTPassword string `json:"mqttPassword"`
	SyncInterval string `json:"syncInterval"`
}

var appConfig AppConfig

func init() {
	if file, err := os.Open("config.json"); err != nil {
		log.Panic(err)
	} else {
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&appConfig); err != nil {
			log.Panic("[CONFIG]", "Fail to load config file JSON", err)
		}
	}

	loadEnvVariables()

	//settingsUrl := appConfig.BackendBaseUrl + "/project/" + appConfig.ProjectId + "/settings"
	//log.Println("loading configuration at:", settingsUrl)
	//if resp, err := http.Get(settingsUrl + "?token=" + appConfig.Token); err != nil {
	//	log.Panic(err)
	//} else {
	//	defer resp.Body.Close()
	//	if resp.StatusCode != 200 {
	//		log.Println("server return invalid response: ", resp.StatusCode)
	//		log.Println("using local settings")
	//	} else {
	//		if err := json.NewDecoder(resp.Body).Decode(&appConfig.RemoteSettings); err != nil {
	//			log.Panic("fail to decode remote settings")
	//		}
	//		log.Println("remote settings loaded successfully")
	//	}
	//}

	log.Println("[CONFIG]", "Config loaded successfully")
}

func loadEnvVariables()  {
	if envValue := os.Getenv("MQTT_BROKER"); envValue != "" {
		appConfig.RemoteSettings.MQTTBroker = envValue
	}
	if envValue := os.Getenv("PROJECT_ID"); envValue != "" {
		appConfig.ProjectId = envValue
	}
	if envValue := os.Getenv("BACKEND_BASE_URL"); envValue != "" {
		appConfig.BackendBaseUrl = envValue
	}
	if envValue := os.Getenv("SYNC_INTERVAL"); envValue != "" {
		appConfig.RemoteSettings.SyncInterval = envValue
	}
}

func Config() AppConfig {
	return appConfig
}
