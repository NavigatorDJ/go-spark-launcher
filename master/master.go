package master

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
)

type Master struct {
	Status      bool
	URI         string
	host        string
	port        int
	web_ui_port int
}

func StartMaster() (Master, error) {
	m := Master{Status: false}
	sh, shb := os.LookupEnv("SPARK_HOME")
	mh, mhb := os.LookupEnv("SPARK_MASTER_HOST")
	mp, mpb := os.LookupEnv("SPARK_MASTER_PORT")
	wp, wpb := os.LookupEnv("SPARK_MASTER_WEBUI_PORT")

	if !shb {
		log.Fatal("Error: Variable SPARK_HOME is not set.")
	}

	if !mhb {
		m.host = "127.0.0.1"
	} else {
		m.host = mh
	}

	if !mpb {
		m.port = 7077
	} else {
		mpi, err := strconv.Atoi(mp)
		if err != nil {
			return Master{}, err
		}
		m.port = mpi
	}

	if !wpb {
		wp = "8080"
		m.web_ui_port = 8080
	} else {
		wpi, err := strconv.Atoi(wp)
		if err != nil {
			return Master{}, err
		}
		m.web_ui_port = wpi
	}

	mc := "org.apache.spark.deploy.master.Master"
	sc := fmt.Sprintf("Start %sbin/spark-class.cmd", sh)
	cmd := exec.Command("cmd.exe", "/c", sc, mc, "-h", m.host, "-p", strconv.Itoa(m.port), "--webui-port", strconv.Itoa(m.web_ui_port))
	err := cmd.Run()
	if err != nil {
		return m, err
	}
	m.URI = fmt.Sprintf("spark://%v:%v", m.host, m.port)
	m.Status = true
	return m, nil
}
