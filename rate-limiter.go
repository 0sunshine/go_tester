package main

import "time"

type RateLimiter struct {
	LimitRate int64
	startTime int64
	lastTime  int64
	readCount int64
}

func NewRateLimiter(limitRate int64) *RateLimiter {
	return &RateLimiter{
		LimitRate: limitRate,
		startTime: 0,
		lastTime:  0,
		readCount: 0,
	}
}

func (this *RateLimiter) Limit(incomingBytes int64) {
	if 0 == this.startTime {
		this.startTime = time.Now().UnixMilli()
		this.lastTime = time.Now().UnixMilli()
	}

	for (this.lastTime-this.startTime)*this.LimitRate < this.readCount*1000 {
		time.Sleep(time.Millisecond * 10)
		this.lastTime = time.Now().UnixMilli()
	}

	this.readCount += incomingBytes
	this.lastTime = time.Now().UnixMilli()
}
