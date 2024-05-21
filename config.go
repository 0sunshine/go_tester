package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"os"
)

type PlayYamlConf struct {
	PlayList           string `yaml:"playList"`
	PlayListSelectType int    `yaml:"playListSelectType"`
	UseOffset          int    `yaml:"useOffset"`
	UsePlayback        int    `yaml:"usePlayback"`

	SessMin           int `yaml:"sessMin"`
	SessMax           int `yaml:"sessMax"`
	SessIncreaseSpeed int `yaml:"sessIncreaseSpeed"`

	SessBytesPerSec int `yaml:"sessBytesPerSec"`

	SessContinuousPlayTime int `yaml:"sessContinuousPlayTime"`
	SessPauseTime          int `yaml:"sessPauseTime"`
	SessStopPlayTime       int `yaml:"sessStopPlayTime"`

	SessRepeat int `yaml:"sessRepeat"`
}

type LogYamlConf struct {
	Level int `yaml:"level"`
}

type YamlConf struct {
	Log    LogYamlConf    `yaml:"log"`
	Listen int            `yaml:"listen"`
	Play   []PlayYamlConf `yaml:"play"`
}

var Conf YamlConf

func LoadConf() error {
	f, err := os.Open("./conf.yaml")
	if err != nil {
		fmt.Println(err)
		return err
	}

	b, err := io.ReadAll(f)
	if err != nil {
		fmt.Println(err)
		return err
	}

	err = yaml.Unmarshal(b, &Conf)
	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("%#v\n", Conf)

	//Conf = YamlConf{
	//	Play: []PlayYamlConf{
	//		PlayYamlConf{},
	//	},
	//	Log: LogYamlConf{Level: 5},
	//}
	//bytes, err := yaml.Marshal(Conf)
	//str := string(bytes)
	//if err != nil {
	//	fmt.Println(err)
	//	return err
	//}
	//fmt.Println(str)

	return nil
}
