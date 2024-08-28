package main

import (
	"embed"
	"fmt"
	"log"

	"openem-ingestor/core"

	"github.com/spf13/viper"
	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
)

//go:embed all:frontend/dist
var assets embed.FS

func main() {

	if err := core.ReadConfig(); err != nil {
		panic(fmt.Errorf("Failed to read config file: %w", err))
	}
	log.Printf("Config file used: %s", viper.ConfigFileUsed())
	log.Println(viper.AllSettings())

	config, _ := core.GetConfig()
	// Create an instance of the app structure
	app := NewApp(config)

	// Create application with options
	err := wails.Run(&options.App{
		Title:  "openem-ingestor",
		Width:  1024,
		Height: 768,
		AssetServer: &assetserver.Options{
			Assets: assets,
		},
		BackgroundColour: &options.RGBA{R: 27, G: 38, B: 54, A: 1},
		OnStartup:        app.startup,
		OnBeforeClose:    app.beforeClose,
		Bind: []interface{}{
			app,
		},
	})

	if err != nil {
		println("Error:", err.Error())
	}
}
