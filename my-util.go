package main

import (
	"github.com/sirupsen/logrus"
	"net/url"
	"strings"
)

func ReplaceLastTokenInUrlPath(url_in string, token string) (string, error) {
	u, err := url.Parse(url_in)
	if err != nil {
		logrus.Error(err)
		return "", err
	}

	u.RawQuery = ""
	//url_in里面的？也去掉

	s := strings.Split(u.Path, "/")
	s = append(s[:len(s)-1], token)
	u.Path = strings.Join(s, "/")

	return url.PathUnescape(u.String())
}
