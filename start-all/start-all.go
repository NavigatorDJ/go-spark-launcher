package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

func main() {
	sh, shb := os.LookupEnv("SPARK_HOME")
	smh, smhb := os.LookupEnv("SPARK_MASTER_HOST")
	smp, smpb := os.LookupEnv("SPARK_MASTER_PORT")
	smwp, smwpb := os.LookupEnv("SPARK_MASTER_WEBUI_PORT")

	if !shb {
		log.Fatal("Error: Variable SPARK_HOME is not set.")
	}

	if !smhb {
		smh = "127.0.0.1"
	}

	if !smpb {
		smp = "7077"
	}

	if !smwpb {
		smwp = "8080"
	}
	// fmt.Println(smh, smp, smwp)
	master := "org.apache.spark.deploy.master.Master"
	worker := "org.apache.spark.deploy.worker.Worker"
	sc := fmt.Sprintf("Start %sbin/spark-class.cmd", sh)
	cmd := exec.Command("cmd.exe", "/c", sc, master, "-h", smh, "-p", smp, "--webui-port", smwp)
	cmd.Run()

	cmd = exec.Command("cmd.exe", "/c", sc, worker, fmt.Sprintf("spark://%s:%s", smh, smp))
	cmd.Run()
}
