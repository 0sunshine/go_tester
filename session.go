package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/grafov/m3u8"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Session struct {
	id           string
	currUrl      string
	limitRate    int64
	sessRepeat   int
	workLimiter  IWorkLimiter
	sessDispatch *SessDispatch
	lastTsUrl    string
}

func NewSession(id string, sessDispatch *SessDispatch, limitRate int, sessRepeat int, workLimiter IWorkLimiter) *Session {
	return &Session{
		id:           id,
		currUrl:      "",
		limitRate:    int64(limitRate),
		sessRepeat:   sessRepeat,
		workLimiter:  workLimiter,
		sessDispatch: sessDispatch,
		lastTsUrl:    "",
	}
}

type TsSegment struct {
	url    string
	extinf int64 //播放时长毫秒
}

var client *fasthttp.Client

func init() {
	// You may read the timeouts from some config
	readTimeout, _ := time.ParseDuration("10000ms")
	writeTimeout, _ := time.ParseDuration("10000ms")
	maxIdleConnDuration, _ := time.ParseDuration("1h")
	client = &fasthttp.Client{
		ReadTimeout:                   readTimeout,
		WriteTimeout:                  writeTimeout,
		MaxIdleConnDuration:           maxIdleConnDuration,
		NoDefaultUserAgentHeader:      true, // Don't send: User-Agent: fasthttp
		DisableHeaderNamesNormalizing: true, // If you set the case on your headers correctly you can enable this
		DisablePathNormalizing:        true,
		// increase DNS cache time to an hour instead of default minute
		Dial: (&fasthttp.TCPDialer{
			Concurrency:      10000,
			DNSCacheDuration: time.Hour,
		}).Dial,
	}
}

func sendGetRequest() {
	req := fasthttp.AcquireRequest()
	req.SetRequestURI("http://localhost:8080/")
	req.Header.SetMethod(fasthttp.MethodGet)
	resp := fasthttp.AcquireResponse()

	err := client.Do(req, resp)
	fasthttp.ReleaseRequest(req)
	if err == nil {
		fmt.Printf("DEBUG Response: %s\n", resp.Body())
	} else {
		fmt.Fprintf(os.Stderr, "ERR Connection error: %v\n", err)
	}
	fasthttp.ReleaseResponse(resp)
}

func httpConnError(err error) (string, bool) {
	var (
		errName string
		known   = true
	)

	switch {
	case errors.Is(err, fasthttp.ErrTimeout):
		errName = "timeout"
	case errors.Is(err, fasthttp.ErrNoFreeConns):
		errName = "conn_limit"
	case errors.Is(err, fasthttp.ErrConnectionClosed):
		errName = "conn_close"
	case reflect.TypeOf(err).String() == "*net.OpError":
		errName = "timeout"
	default:
		known = false
	}

	return errName, known
}

func (sess *Session) doDownloadTs(ts_url string) error {
	logrus.Info("id:[", sess.id, "]--download ts: ", ts_url)

	req := fasthttp.AcquireRequest()
	req.SetRequestURI(ts_url)
	req.Header.SetMethod(fasthttp.MethodGet)
	resp := fasthttp.AcquireResponse()

	err := client.Do(req, resp)
	fasthttp.ReleaseRequest(req)

	if err == nil {
		logrus.Info("download ts ok, filesize: ", len(resp.Body()))
	} else {
		logrus.Error("id:]", sess.id, "]--Failed to download the file. HTTP Status Code: ", resp.StatusCode())
		return errors.New("err: " + strconv.Itoa(resp.StatusCode()))
	}
	fasthttp.ReleaseResponse(resp)

	return nil
}

func (sess *Session) doDownloadUrl() error {
	resp, err := http.Get(sess.currUrl)
	if err != nil {
		logrus.Error(err)
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusFound {
		location, err := resp.Location()
		if err != nil {
			logrus.Error(err)
			return err
		}

		sess.currUrl = location.String()

		return nil
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("err: " + strconv.Itoa(resp.StatusCode))
	}

	p, listType, err := m3u8.DecodeWith(bufio.NewReader(resp.Body), true, []m3u8.CustomDecoder{})

	if err != nil {
		logrus.Error(err)
		return err
	}

	sub_m3u8 := ""

	if listType == m3u8.MASTER {
		masterpl := p.(*m3u8.MasterPlaylist)
		for _, v := range masterpl.Variants {
			sub_m3u8 = v.URI
			break //只取第一条子m3u8
		}

		sess.currUrl, err = ReplaceLastTokenInUrlPath(sess.currUrl, sub_m3u8)
		if err != nil {
			logrus.Error(err)
			return err
		}

		return nil
	}

	atomic.AddInt64(&DSsessionOnlineNum, 1)

	//开始下载每一个ts
	mediapl := p.(*m3u8.MediaPlaylist)

	argIdx := strings.LastIndex(sess.currUrl, "?")
	replaceUrl := sess.currUrl
	if argIdx != -1 {
		replaceUrl = replaceUrl[:argIdx]
	}

	//下载的ts切片列表
	tsSegmentList := []TsSegment{}

	//直播多次获取m3u8，移除已经下载过的
	for _, seg := range mediapl.Segments {
		if seg == nil {
			continue
		}

		ts_url, err := ReplaceLastTokenInUrlPath(replaceUrl, seg.URI)
		if err != nil {
			logrus.Error(err)
			break
		}

		if ts_url == sess.lastTsUrl {
			tsSegmentList = []TsSegment{} //去除前面已经下载过的
			continue
		}

		tsSegment := TsSegment{
			url:    ts_url,
			extinf: -1,
		}

		tsSegment.extinf = int64(seg.Duration * 1000)
		tsSegmentList = append(tsSegmentList, tsSegment)
	}

	if len(tsSegmentList) == 0 {
		time.Sleep(time.Millisecond * 200) //没有切片, 等200ms再请求
	}

	for _, tsSegment := range tsSegmentList {

		sess.lastTsUrl = tsSegment.url

		sess.workLimiter.WorkWait()

		tsDownloadStartTime := time.Now().UnixMilli()
		err = sess.doDownloadTs(tsSegment.url)
		tsDownloadEndTime := time.Now().UnixMilli()
		tsDownloadMs := tsDownloadEndTime - tsDownloadStartTime
		atomic.AddInt64(&DStsDownloadTotalNum, 1)

		if err != nil {
			atomic.AddInt64(&DStsDownloadFailedNum, 1)
			AddTsDownloadSpeedTime(err, tsDownloadMs)
			logrus.Error(err)
			continue
		}

		AddTsDownloadSpeedTime(err, tsDownloadMs)
		atomic.AddInt64(&DStsDownloadSuccessNum, 1)
		if tsDownloadMs < tsSegment.extinf {
			time.Sleep(time.Millisecond * time.Duration(tsSegment.extinf-tsDownloadMs))
		}
	}

	atomic.AddInt64(&DSsessionOnlineNum, -1)

	//live
	if mediapl.Closed == false {
		return nil
	} else {
		sess.currUrl = ""
	}

	return nil
}

func (sess *Session) Do() {
	var err error

	for {
		if len(sess.currUrl) == 0 {
			sess.currUrl, err = sess.sessDispatch.getUrlFromPlayList()
			if err != nil {
				logrus.Error(err)
				break
			}
		}

		sess.doDownloadUrl()

		if sess.sessRepeat == 0 {
			break
		}
	}
}
