package cliutils

import (
	"log"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

type EnvironmentConfig struct {
	APIServer string
	Env       string
}

type ArchiveConfig struct {
	APIServer   string
	RSYNCServer string
	Env         string
}

type RetrieveConfig struct {
	APIServer   string
	RSYNCServer string
	Env         string
}

type InputEnvironmentConfig struct {
	TestenvFlag   bool
	DevenvFlag    bool
	TunnelenvFlag bool
	LocalenvFlag  bool
	ScicatUrl     string
	RsyncUrl      string
}

func returnCommonEnvironmentFlags(cfg InputEnvironmentConfig) EnvironmentConfig {
	config := EnvironmentConfig{
		APIServer: PROD_API_SERVER,
		Env:       "production",
	}
	if cfg.TunnelenvFlag {
		config.APIServer = TUNNEL_API_SERVER
		config.Env = "dev"
	}
	if cfg.LocalenvFlag {
		config.APIServer = LOCAL_API_SERVER
		config.Env = "local"
	}
	if cfg.DevenvFlag {
		config.APIServer = DEV_API_SERVER
		config.Env = "dev"
	}
	if cfg.TestenvFlag {
		config.APIServer = TEST_API_SERVER
		config.Env = "test"
	}
	if cfg.ScicatUrl != "" {
		config.APIServer = cfg.ScicatUrl
		config.Env = "custom"
	}
	color.Set(color.FgGreen)
	log.Printf("You are about to add a dataset to the === %s === data catalog environment...", config.Env)
	color.Unset()

	return config
}

func applyArchiveRSYNCFlags(config *ArchiveConfig, cfg InputEnvironmentConfig) {
	if cfg.TunnelenvFlag {
		config.RSYNCServer = TUNNEL_RSYNC_ARCHIVE_SERVER
	}
	if cfg.LocalenvFlag {
		config.RSYNCServer = LOCAL_RSYNC_ARCHIVE_SERVER
	}
	if cfg.DevenvFlag {
		config.RSYNCServer = DEV_RSYNC_ARCHIVE_SERVER
	}
	if cfg.TestenvFlag {
		config.RSYNCServer = TEST_RSYNC_ARCHIVE_SERVER
	}
	if cfg.ScicatUrl != "" && cfg.RsyncUrl != "" {
		config.RSYNCServer = cfg.RsyncUrl
	}
}

// ConfigureEnvironment sets the APIServer and env based on provided flags.
// Production is the default, can be overridden by tunnel, local, dev, test, or scicatUrl.
func ConfigureEnvironment(cfg InputEnvironmentConfig) string {
	return returnCommonEnvironmentFlags(cfg).APIServer
}

// ConfigureArchiveEnvironment sets the APIServer, RSYNCServer and env for archive operations.
// Production is the default, can be overridden by tunnel, local, dev, test, or scicatUrl.
// If scicatUrl is provided with rsyncUrl, both are set to custom; otherwise uses custom-{env}.
func ConfigureArchiveEnvironment(cfg InputEnvironmentConfig) (apiserver string, rsyncserver string) {
	commonConfig := returnCommonEnvironmentFlags(cfg)
	config := ArchiveConfig{
		APIServer:   commonConfig.APIServer,
		RSYNCServer: PROD_RSYNC_ARCHIVE_SERVER,
		Env:         commonConfig.Env,
	}
	applyArchiveRSYNCFlags(&config, cfg)

	return config.APIServer, config.RSYNCServer
}

func GetCobraBoolFlag(cmd *cobra.Command, name string) bool {
	val, _ := cmd.Flags().GetBool(name)
	return val
}

func GetCobraStringFlag(cmd *cobra.Command, name string) string {
	val, _ := cmd.Flags().GetString(name)
	return val
}

func GetCobraIntFlag(cmd *cobra.Command, name string) int {
	val, _ := cmd.Flags().GetInt(name)
	return val
}
