package main

import (
	"time"
)

type IWorkLimiter interface {
	WorkWait()
}

type WorkLimiter struct {
	SessContinuousPlayTime int
	SessPauseTime          int

	beginWorkTime int64
}

func (this *WorkLimiter) WorkWait() {
	if this.beginWorkTime == 0 {
		this.beginWorkTime = time.Now().Unix()
		return
	}

	if time.Now().Unix()-this.beginWorkTime > int64(this.SessContinuousPlayTime) {
		time.Sleep(time.Duration(this.SessPauseTime) * time.Second)
		this.beginWorkTime = time.Now().Unix()
	}
}
