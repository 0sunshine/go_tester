package main

import (
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"os/signal"
	"syscall"
)

var (
	//SessionNumFlag int
	version string = "v1.1.0"
)

func ini_flag() bool {
	//flag.IntVar(&SessionNumFlag, "session_num", 100, "启动的连接会话数")
	var showVer bool
	flag.BoolVar(&showVer, "v", false, "print version")
	flag.Parse()

	if showVer {
		fmt.Println("version: ", version)
		return false
	}

	//fmt.Println("session_num: ", SessionNumFlag)

	return true
}

var logFile *lumberjack.Logger

func init_log() {
	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	logFile = &lumberjack.Logger{
		Filename:   "go_test_log.txt",
		MaxSize:    50, // MB
		MaxBackups: 10,
		MaxAge:     28, // days
		Compress:   false,
	}

	logrus.SetOutput(logFile)
	logrus.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	logrus.SetLevel(logrus.Level(Conf.Log.Level))

	logrus.SetReportCaller(true)
}

func waitForQuit() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		fmt.Println(sig)
		done <- true
	}()

	<-done

	fmt.Println("quit ....")
}

func main() {
	if !ini_flag() {
		return
	}

	err := LoadConf()
	if err != nil {
		return
	}

	init_log()
	defer func() {
		err := logFile.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	DoSessDispatch()
	go StartBackendWebServer()

	waitForQuit()
}
