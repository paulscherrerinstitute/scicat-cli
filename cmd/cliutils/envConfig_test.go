package cliutils

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestResolveAPIServer(t *testing.T) {
	tests := []struct {
		name  string
		input InputEnvironmentConfig
		want  string
	}{
		{
			name:  "production default",
			input: InputEnvironmentConfig{},
			want:  PROD_API_SERVER,
		},
		{
			name:  "dev environment",
			input: InputEnvironmentConfig{DevenvFlag: true},
			want:  DEV_API_SERVER,
		},
		{
			name:  "test environment",
			input: InputEnvironmentConfig{TestenvFlag: true},
			want:  TEST_API_SERVER,
		},
		{
			name:  "tunnel environment",
			input: InputEnvironmentConfig{TunnelenvFlag: true},
			want:  TUNNEL_API_SERVER,
		},
		{
			name:  "custom scicat url",
			input: InputEnvironmentConfig{ScicatUrl: "https://custom.example.com/api/v3"},
			want:  "https://custom.example.com/api/v3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.ResolveAPIServer()
			if got != tt.want {
				t.Errorf("ResolveAPIServer() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestResolveRSYNCServer(t *testing.T) {
	tests := []struct {
		name  string
		input InputEnvironmentConfig
		want  string
	}{
		{
			name:  "production default",
			input: InputEnvironmentConfig{},
			want:  PROD_RSYNC_ARCHIVE_SERVER,
		},
		{
			name:  "dev environment",
			input: InputEnvironmentConfig{DevenvFlag: true},
			want:  DEV_RSYNC_ARCHIVE_SERVER,
		},
		{
			name:  "test environment",
			input: InputEnvironmentConfig{TestenvFlag: true},
			want:  TEST_RSYNC_ARCHIVE_SERVER,
		},
		{
			name: "custom rsync url",
			input: InputEnvironmentConfig{
				ScicatUrl: "https://custom.example.com/api/v3",
				RsyncUrl:  "custom-rsync.example.com",
			},
			want: "custom-rsync.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.ResolveRSYNCServer()
			if got != tt.want {
				t.Errorf("ResolveRSYNCServer() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestCobraFlagHelpers(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().Bool("verbose", false, "")
	cmd.Flags().String("name", "default", "")
	cmd.Flags().Int("count", 0, "")

	t.Run("GetCobraBoolFlag", func(t *testing.T) {
		cmd.Flags().Set("verbose", "true")
		if GetCobraBoolFlag(cmd, "verbose") != true {
			t.Error("Expected true")
		}
	})

	t.Run("GetCobraStringFlag", func(t *testing.T) {
		cmd.Flags().Set("name", "scicat-user")
		if GetCobraStringFlag(cmd, "name") != "scicat-user" {
			t.Error("Expected scicat-user")
		}
	})

	t.Run("GetCobraIntFlag", func(t *testing.T) {
		cmd.Flags().Set("count", "42")
		if GetCobraIntFlag(cmd, "count") != 42 {
			t.Error("Expected 42")
		}
	})

	t.Run("MissingFlagsReturnDefaults", func(t *testing.T) {
		if GetCobraBoolFlag(cmd, "non-existent") != false {
			t.Error("Expected false")
		}
		if GetCobraStringFlag(cmd, "non-existent") != "" {
			t.Error("Expected empty string")
		}
		if GetCobraIntFlag(cmd, "non-existent") != 0 {
			t.Error("Expected 0")
		}
	})
}
