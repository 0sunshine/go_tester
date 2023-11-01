package main

import (
	"bufio"
	"errors"
	"github.com/grafov/m3u8"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
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
	}

	limiter := NewRateLimiter(sess.limitRate)

	// Read and discard the content
	buf := make([]byte, 1024*4) //4k
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
	tsUrlList := []string{}

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
			tsUrlList = []string{}
			continue
		}

		tsUrlList = append(tsUrlList, ts_url)
	}

	if len(tsUrlList) == 0 {
		time.Sleep(time.Second * 5)
	}

	for _, ts_url := range tsUrlList {

		sess.lastTsUrl = ts_url

		sess.workLimiter.WorkWait()

		tsDownloadStartTime := time.Now().UnixMilli()
		err = sess.doDownloadTs(ts_url)
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
	}

	atomic.AddInt64(&DSsessionOnlineNum, -1)

	//live
	if mediapl.MediaType == m3u8.EVENT {
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
