package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/uol/gobol/election"
	"github.com/uol/gobol/saw"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

//
// Does the election process using the election package
// author: rnojiri
//

func main() {

	logger, err := saw.New("INFO", "QA")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(0)
	}

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "main"),
	}

	cfg := election.Config{
		ZKURL:                  []string{"zookeeper.intranet"},
		ZKElectionNodeURI:      "/master",
		ZKSlaveNodesURI:        "/slaves",
		ReconnectionTimeout:    3,
		SessionTimeout:         5,
		ClusterChangeCheckTime: 1000,
	}

	manager, err := election.New(&cfg, logger)
	if err != nil {
		logger.Error(err.Error(), lf...)
		os.Exit(0)
	}

	feedbackChannel, err := manager.Start()

	go func() {
		for {
			select {
			case signal := <-*feedbackChannel:
				if signal == election.Master {
					logger.Info("master signal received", lf...)
				} else if signal == election.Slave {
					logger.Info("slave signal received", lf...)
				} else if signal == election.ClusterChanged {
					logger.Info("cluster changed signal received", lf...)
				}
			}
		}
	}()

	ci, err := manager.GetClusterInfo()
	if err != nil {
		logger.Error(err.Error(), lf...)
		os.Exit(0)
	}

	logger.Info(fmt.Sprintf("%+v", ci), lf...)

	var gracefulStop = make(chan os.Signal)
	signal.Notify(gracefulStop, syscall.SIGTERM)
	signal.Notify(gracefulStop, syscall.SIGINT)

	go func() {
		<-gracefulStop
		logger.Error("exiting...", lf...)
		manager.Terminate()
		time.Sleep(2 * time.Second)
		os.Exit(0)
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
