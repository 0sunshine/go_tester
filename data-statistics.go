package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
)

var DSsessionOnlineNum int64 = 0
var DStsDownloadSuccessNum int64 = 0
var DStsDownloadFailedNum int64 = 0
var DStsDownloadTotalNum int64 = 0

var mutex sync.Mutex

var tsDownloadSpeedTimeTotalms int64 = 0
var tsDownloadSpeedTimeErrTotalms int64 = 0

var tsDownloadSpeedTimers = []*tsDownloadSpeedTimer{
	&tsDownloadSpeedTimer{
		start: 0,
		end:   2000,
		count: 0,
	},
	&tsDownloadSpeedTimer{
		start: 2000,
		end:   5000,
		count: 0,
	},
	&tsDownloadSpeedTimer{
		start: 5000,
		end:   8000,
		count: 0,
	},
	&tsDownloadSpeedTimer{
		start: 8000,
		end:   10000,
		count: 0,
	},
	&tsDownloadSpeedTimer{
		start: 10000,
		end:   -1,
		count: 0,
	},
}

type tsDownloadSpeedTimer struct {
	start int64
	end   int64
	count int64
}

func (this *tsDownloadSpeedTimer) handle(ms int64) {
	if this.start == -1 || ms >= this.start {
		if this.end == -1 || ms <= this.end {
			(this.count)++
		}
	}
}

func showPercent(a int64, b int64) string {
	if b == 0 {
		return "-"
	}

	return fmt.Sprintf("%.2f%%", float64(a)/float64(b)*100)
}

func (this *tsDownloadSpeedTimer) show(w http.ResponseWriter) {
	strStart := "-"
	strEnd := "-"

	if this.start != -1 {
		strStart = strconv.FormatInt(this.start, 10)
	}

	if this.end != -1 {
		strEnd = strconv.FormatInt(this.end, 10)
	}

	if DStsDownloadTotalNum != 0 {
		fmt.Fprintf(w, "成功ts耗时(%s~%s)：%d (%s)\n", strStart, strEnd, this.count, showPercent(this.count, DStsDownloadTotalNum))
	}
}

func init() {

}

func handler(w http.ResponseWriter, r *http.Request) {
	mutex.Lock()
	defer mutex.Unlock()

	fmt.Fprintf(w, "在线会话数：%d\n", DSsessionOnlineNum)
	fmt.Fprintf(w, "成功下载ts数：%d (%s)  下载失败ts数:%d (%s) 总ts数: %d\n",
		DStsDownloadSuccessNum,
		showPercent(DStsDownloadSuccessNum, DStsDownloadTotalNum),
		DStsDownloadFailedNum,
		showPercent(DStsDownloadFailedNum, DStsDownloadTotalNum),
		DStsDownloadTotalNum)

	if DStsDownloadSuccessNum != 0 {
		fmt.Fprintf(w, "成功ts下载平均耗时ms：%d \n", tsDownloadSpeedTimeTotalms/DStsDownloadSuccessNum)
	}

	if DStsDownloadFailedNum != 0 {
		fmt.Fprintf(w, "失败ts下载平均耗时ms：%d \n", tsDownloadSpeedTimeErrTotalms/DStsDownloadFailedNum)
	}

	for _, obj := range tsDownloadSpeedTimers {
		obj.show(w)
	}
}

func StartBackendWebServer() {
	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(":18080", nil))
}

func AddTsDownloadSpeedTime(err error, ms int64) {
	mutex.Lock()
	defer mutex.Unlock()

	if err != nil {
		tsDownloadSpeedTimeErrTotalms += ms
		return
	}

	tsDownloadSpeedTimeTotalms += ms

	for _, obj := range tsDownloadSpeedTimers {
		obj.handle(ms)
	}
}
