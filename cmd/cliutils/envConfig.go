package cliutils

import (
	"log"

	"github.com/fatih/color"
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

func returnCommonEnvironmentFlags(tunnelenvFlag, localenvFlag, devenvFlag, testenvFlag bool, scicatUrl string) EnvironmentConfig {
	config := EnvironmentConfig{
		APIServer: PROD_API_SERVER,
		Env:       "production",
	}
	if tunnelenvFlag {
		config.APIServer = TUNNEL_API_SERVER
		config.Env = "dev"
	}
	if localenvFlag {
		config.APIServer = LOCAL_API_SERVER
		config.Env = "local"
	}
	if devenvFlag {
		config.APIServer = DEV_API_SERVER
		config.Env = "dev"
	}
	if testenvFlag {
		config.APIServer = TEST_API_SERVER
		config.Env = "test"
	}
	if scicatUrl != "" {
		config.APIServer = scicatUrl
		config.Env = "custom"
	}
	color.Set(color.FgGreen)
	log.Printf("You are about to add a dataset to the === %s === data catalog environment...", config.Env)
	color.Unset()

	return config
}

func applyArchiveRSYNCFlags(config *ArchiveConfig, tunnelenvFlag, localenvFlag, devenvFlag, testenvFlag bool, scicatUrl string, rsyncUrl string) {
	if tunnelenvFlag {
		config.RSYNCServer = TUNNEL_RSYNC_ARCHIVE_SERVER
	}
	if localenvFlag {
		config.RSYNCServer = LOCAL_RSYNC_ARCHIVE_SERVER
	}
	if devenvFlag {
		config.RSYNCServer = DEV_RSYNC_ARCHIVE_SERVER
	}
	if testenvFlag {
		config.RSYNCServer = TEST_RSYNC_ARCHIVE_SERVER
	}
	if scicatUrl != "" && rsyncUrl != "" {
		config.RSYNCServer = rsyncUrl
	}
}

// ConfigureEnvironment sets the APIServer and env based on provided flags.
// Production is the default, can be overridden by tunnel, local, dev, test, or scicatUrl.
func ConfigureEnvironment(tunnelenvFlag, localenvFlag, devenvFlag, testenvFlag bool, scicatUrl string) EnvironmentConfig {
	return returnCommonEnvironmentFlags(tunnelenvFlag, localenvFlag, devenvFlag, testenvFlag, scicatUrl)
}

// ConfigureArchiveEnvironment sets the APIServer, RSYNCServer and env for archive operations.
// Production is the default, can be overridden by tunnel, local, dev, test, or scicatUrl.
// If scicatUrl is provided with rsyncUrl, both are set to custom; otherwise uses custom-{env}.
func ConfigureArchiveEnvironment(tunnelenvFlag, localenvFlag, devenvFlag, testenvFlag bool, scicatUrl, rsyncUrl string) ArchiveConfig {
	commonConfig := returnCommonEnvironmentFlags(tunnelenvFlag, localenvFlag, devenvFlag, testenvFlag, "")
	config := ArchiveConfig{
		APIServer:   commonConfig.APIServer,
		RSYNCServer: PROD_RSYNC_ARCHIVE_SERVER,
		Env:         commonConfig.Env,
	}
	applyArchiveRSYNCFlags(&config, tunnelenvFlag, localenvFlag, devenvFlag, testenvFlag, scicatUrl, rsyncUrl)

	return config
}
