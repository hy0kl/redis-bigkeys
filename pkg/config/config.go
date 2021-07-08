package config

import (
	"fmt"

	"gopkg.in/ini.v1"
)

var cfg *ini.File

func init() {
	cfgFile := `./conf/app.ini`
	load, err := ini.Load(cfgFile)
	if err != nil {
		panic(fmt.Sprintf(`can open config file: %s`, cfgFile))
	}

	cfg = load
}

func GetCfg() *ini.File {
	return cfg
}
