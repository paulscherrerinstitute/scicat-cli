package core

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type Scicat struct {
	Host        string `string:"Host"`
	AccessToken string `string:"AccessToken"`
}

type S3 struct {
	Endpoint string `string:"Endpoint"`
	Bucket   string `string:"Bucket"`
	Checksum bool   `bool:"Checksum"`
}

type Globus struct {
	Endpoint string `string:"Endpoint"`
}

type Transfer struct {
	Method string `string:"Method"`
	S3     S3     `mapstructure:"S3"`
	Globus Globus `mapstructure:"Globus"`
}

type Misc struct {
	ConcurrencyLimit int `int:"ConcurrencyLimit"`
}

type Config struct {
	Scicat   Scicat   `mapstructure:"Scicat"`
	Transfer Transfer `mapstructure:"Transfer"`
	Misc     Misc     `mapstructure:"Misc"`
}

func GetConfig() (Config, error) {
	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		fmt.Println(err)
		return config, err
	}
	return config, nil
}

func ReadConfig() error {
	viper.SetConfigName("openem-ingestor-config") // name of config file (without extension)
	viper.SetConfigType("yaml")

	userConfigDir, _ := os.UserConfigDir()
	viper.AddConfigPath(userConfigDir)

	// these paths are mostly for development
	viper.AddConfigPath("./")
	viper.AddConfigPath("./core")

	err := viper.ReadInConfig()
	return err
}
