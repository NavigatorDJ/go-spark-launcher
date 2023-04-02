package main

import (
	"log"

	"github.com/NavigatorDJ/go-spark-launcher/master"
	"github.com/NavigatorDJ/go-spark-launcher/worker"
)

func main() {
	log.Println("Start")
	m, err := master.StartMaster()

	if err != nil {
		log.Fatal(err)
	} else {
		log.Println(m.URI)
		log.Println(m.Status)
	}

	err = worker.StartWorker(m)
	if err != nil {
		log.Fatal(err)
	}
}
