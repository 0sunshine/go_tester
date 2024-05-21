package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/grafov/m3u8"
	"github.com/sirupsen/logrus"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Session struct {
	id               string
	currUrl          string
	limitRate        int64
	sessRepeat       int
	useOffset        int
	usePlayback      int
	sessStopPlayTime int

	workLimiter  IWorkLimiter
	sessDispatch *SessDispatch
	lastTsUrl    string

	currOnlineTime int
}

func NewSession(id string, sessDispatch *SessDispatch, limitRate int, sessRepeat int, workLimiter IWorkLimiter, useOffset int, usePlayback int, sessStopPlayTime int) *Session {
	return &Session{
		id:               id,
		currUrl:          "",
		limitRate:        int64(limitRate),
		sessRepeat:       sessRepeat,
		workLimiter:      workLimiter,
		sessDispatch:     sessDispatch,
		lastTsUrl:        "",
		useOffset:        useOffset,
		usePlayback:      usePlayback,
		sessStopPlayTime: sessStopPlayTime,
	}
}

type TsSegment struct {
	url    string
	extinf int64 //播放时长毫秒
}

func (sess *Session) doDownloadTs(ts_url string) error {
	logrus.Info("id:[", sess.id, "]--download ts: ", ts_url)

	resp, err := http.Get(ts_url)
	if err != nil {
		logrus.Error("id:[", sess.id, "]--Error downloading: ", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logrus.Error("id:]", sess.id, "]--Failed to download the file. HTTP Status Code: ", resp.StatusCode)
		return errors.New("err: " + strconv.Itoa(resp.StatusCode))
	}

	limiter := NewRateLimiter(sess.limitRate)

	// Read and discard the content
	buf := make([]byte, 1024*512) //64k
	for {
		n, err := resp.Body.Read(buf)

		if err == io.EOF {
			break
		} else if err != nil {
			logrus.Error("[id:", sess.id, "]--Failed to download, err: ", err)
			return err
		}

		limiter.Limit(int64(n))
	}

	return nil
}

func (sess *Session) doDownloadFile() error {
	logrus.Info("id:[", sess.id, "]--download file: ", sess.currUrl)

	atomic.AddInt64(&DSsessionOnlineNum, 1)
	defer atomic.AddInt64(&DSsessionOnlineNum, -1)
	defer atomic.AddInt64(&DStsDownloadTotalNum, 1)

	resp, err := http.Get(sess.currUrl)
	if err != nil {
		logrus.Error(err)
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		atomic.AddInt64(&DStsDownloadFailedNum, 1)
		logrus.Error("id:]", sess.id, "]--Failed to download the file. HTTP Status Code: ", resp.StatusCode)
		return errors.New("err: " + strconv.Itoa(resp.StatusCode))
	}

	limiter := NewRateLimiter(sess.limitRate)

	// Read and discard the content
	buf := make([]byte, 1024*512) //64k
	for {
		n, err := resp.Body.Read(buf)

		if err == io.EOF {
			break
		} else if err != nil {
			logrus.Error("[id:", sess.id, "]--Failed to download, err: ", err)
			atomic.AddInt64(&DStsDownloadFailedNum, 1)
			return err
		}

		limiter.Limit(int64(n))
	}

	atomic.AddInt64(&DStsDownloadSuccessNum, 1)

	return nil
}

func (sess *Session) doDownloadHlsUrl() error {

	if strings.Contains(sess.currUrl, "offset=") == false && strings.Contains(sess.currUrl, "start=") == false && strings.Contains(sess.currUrl, "end=") == false {
		if sess.useOffset > 0 {
			r := rand.Int()%sess.useOffset + 1
			if strings.Contains(sess.currUrl, "?") {
				sess.currUrl += "&"
			} else {
				sess.currUrl += "?"
			}
			sess.currUrl += fmt.Sprintf("offset=%d", r)

		} else if sess.usePlayback > 0 {
			nowTmp := time.Now()
			r := rand.Int() % sess.usePlayback

			startTmp := nowTmp.Add(-time.Second * time.Duration(r))
			endTmp := startTmp.Add(time.Second * 15 * 60)

			if endTmp.After(nowTmp) {
				endTmp = nowTmp
			}

			if strings.Contains(sess.currUrl, "?") {
				sess.currUrl += "&"
			} else {
				sess.currUrl += "?"
			}

			sess.currUrl += fmt.Sprintf("start=%s&end=%s", startTmp.Format("20060102150405"), endTmp.Format("20060102150405"))
		}
	}

	logrus.Info("id:[", sess.id, "]--download m3u8: ", sess.currUrl)

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	//m3u8Url := sess.currUrl

	req, err := http.NewRequest("GET", sess.currUrl, nil)

	resp, err := client.Do(req)

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
		logrus.Info("id:[", sess.id, "]--302 to: ", sess.currUrl)

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

		sess.currOnlineTime += int(tsSegment.extinf)

		//到点退出, 换播放串
		if sess.currOnlineTime/1000 > sess.sessStopPlayTime {
			logrus.Info("id:[", sess.id, "]--sessStopPlayTime")
			sess.currUrl = ""
			sess.currOnlineTime = 0
			return nil
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

		if strings.Contains(sess.currUrl, ".m3u8") {
			err = sess.doDownloadHlsUrl()
		} else {
			err = sess.doDownloadFile()
		}

		if err != nil {
			time.Sleep(time.Second * 1)
			logrus.Info("id:[", sess.id, "]--err : ", err, "  url: ", sess.currUrl)
		}

	}
}
