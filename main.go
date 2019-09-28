package main

import (
	"encoding/json"
	"face-recognizer-worker/config"
	"face-recognizer-worker/worker"
	"github.com/ndphu/swd-commons/model"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

var workers = make([]*worker.Worker, 0)
var workerLocks = sync.Mutex{}

func main() {
	go func() {
		for {
			reloadProjects()
			time.Sleep(5 * time.Minute)
		}
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	log.Println("Interrupt signal received. Exiting...")
	for _, w := range workers {
		w.Stop()
	}
	os.Exit(0)
}

func reloadProjects() {
	projects, err := getProjects()
	if err != nil {
		log.Println("[APP]", "Fail to load project list", err.Error())
		return
	}
	workerLocks.Lock()
	for _, w := range workers {
		w.Stop()
	}
	for _, project := range projects {
		newWorker := worker.NewWorker(project.ProjectId)
		workers = append(workers, newWorker)
		go newWorker.Run()
	}
	workerLocks.Unlock()
}

func getProjects() ([]model.Project, error) {
	reloadUrl := config.Config().BackendBaseUrl + "/projects?token=" + config.Config().Token
	log.Println("[WORKER]", "Reload URL:", reloadUrl)
	if resp, err := http.Get(reloadUrl); err != nil {
		return nil, err
	} else {
		defer resp.Body.Close()
		projects := make([]model.Project, 0)
		if body, err := ioutil.ReadAll(resp.Body); err != nil {
			return nil, err
		} else {
			err = json.Unmarshal(body, &projects)
			return projects, err
		}
	}
}
