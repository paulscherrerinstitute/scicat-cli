package main

import (
	"fmt"
	"log"
	"openem-ingestor/core"

	"github.com/spf13/viper"
)

func main() {
	if err := core.ReadConfig(); err != nil {
		panic(fmt.Errorf("Failed to read config file: %w", err))
	}
	log.Printf("Config file used: %s", viper.ConfigFileUsed())
	log.Println(viper.AllSettings())

}
