package cliutils

import (
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

type InputEnvironmentConfig struct {
	TestenvFlag   bool
	DevenvFlag    bool
	TunnelenvFlag bool
	LocalenvFlag  bool
	ScicatUrl     string
	RsyncUrl      string
}

type EnvironmentConfig struct {
	APIServer string
	Env       string
}

func (c *InputEnvironmentConfig) getBaseConfig() EnvironmentConfig {
	config := EnvironmentConfig{
		APIServer: PROD_API_SERVER,
		Env:       "production",
	}
	if c.TunnelenvFlag {
		config.APIServer = TUNNEL_API_SERVER
		config.Env = "dev"
	}
	if c.LocalenvFlag {
		config.APIServer = LOCAL_API_SERVER
		config.Env = "local"
	}
	if c.DevenvFlag {
		config.APIServer = DEV_API_SERVER
		config.Env = "dev"
	}
	if c.TestenvFlag {
		config.APIServer = TEST_API_SERVER
		config.Env = "test"
	}
	if c.ScicatUrl != "" {
		config.APIServer = c.ScicatUrl
		config.Env = "custom"
	}

	return config
}
func (c *InputEnvironmentConfig) ResolveAPIServer() string {
	cfg := c.getBaseConfig()
	color.Green("You are about to interact with the === %s === data catalog environment...", cfg.Env)
	return cfg.APIServer
}

func (c *InputEnvironmentConfig) ResolveRSYNCServer() string {
	if c.RsyncUrl != "" && c.ScicatUrl != "" {
		return c.RsyncUrl
	}

	if c.TestenvFlag {
		return TEST_RSYNC_ARCHIVE_SERVER
	}
	if c.DevenvFlag {
		return DEV_RSYNC_ARCHIVE_SERVER
	}
	if c.LocalenvFlag {
		return LOCAL_RSYNC_ARCHIVE_SERVER
	}
	if c.TunnelenvFlag {
		return TUNNEL_RSYNC_ARCHIVE_SERVER
	}

	return PROD_RSYNC_ARCHIVE_SERVER
}

// --- Cobra Helper Methods ---
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
