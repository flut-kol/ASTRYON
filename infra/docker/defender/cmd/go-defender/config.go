package main

import (
	"encoding/json"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Port         string `json:"Port" envconfig:"PORT" default:":3993"`
	SharedFolder string `json:"SharedFolder" envconfig:"SHARED_FOLDER" required:"true"`
	LogLevel     string `json:"LogLevel" envconfig:"LOG_LEVEL" default:"info"`
	Debug        bool   `json:"Debug" envconfig:"DEBUG" default:"false"`
	Timeout      int64  `json:"Timeout" envconfig:"TIMEOUT_SECONDS" default:"60"`
}

func ReadConfig() (Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) String() string {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return "can't serialize main config to json"
	}

	return string(b)
}
