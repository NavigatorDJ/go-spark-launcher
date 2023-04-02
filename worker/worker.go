package worker

import (
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/NavigatorDJ/go-spark-launcher/master"
)

func StartWorker(m master.Master) error {
	sh, shb := os.LookupEnv("SPARK_HOME")

	if !shb {
		log.Fatal("Error: Variable SPARK_HOME is not set.")
	}

	wc := "org.apache.spark.deploy.worker.Worker"
	sc := fmt.Sprintf("Start %sbin/spark-class.cmd", sh)

	cmd := exec.Command("cmd.exe", "/c", sc, wc, m.URI)
	err := cmd.Run()
	return err

}
