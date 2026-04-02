package cliutils

import (
	"testing"
)

func TestConfigureEnvironment(t *testing.T) {
	tests := []struct {
		name      string
		tunnel    bool
		local     bool
		dev       bool
		test      bool
		scicatUrl string
		want      EnvironmentConfig
	}{
		{
			name:      "production default",
			tunnel:    false,
			local:     false,
			dev:       false,
			test:      false,
			scicatUrl: "",
			want: EnvironmentConfig{
				APIServer: PROD_API_SERVER,
				Env:       "production",
			},
		},
		{
			name:      "dev environment",
			tunnel:    false,
			local:     false,
			dev:       true,
			test:      false,
			scicatUrl: "",
			want: EnvironmentConfig{
				APIServer: DEV_API_SERVER,
				Env:       "dev",
			},
		},
		{
			name:      "test environment",
			tunnel:    false,
			local:     false,
			dev:       false,
			test:      true,
			scicatUrl: "",
			want: EnvironmentConfig{
				APIServer: TEST_API_SERVER,
				Env:       "test",
			},
		},
		{
			name:      "local environment",
			tunnel:    false,
			local:     true,
			dev:       false,
			test:      false,
			scicatUrl: "",
			want: EnvironmentConfig{
				APIServer: LOCAL_API_SERVER,
				Env:       "local",
			},
		},
		{
			name:      "tunnel environment",
			tunnel:    true,
			local:     false,
			dev:       false,
			test:      false,
			scicatUrl: "",
			want: EnvironmentConfig{
				APIServer: TUNNEL_API_SERVER,
				Env:       "dev",
			},
		},
		{
			name:      "custom scicat url",
			tunnel:    false,
			local:     false,
			dev:       false,
			test:      false,
			scicatUrl: "https://custom.example.com/api/v3",
			want: EnvironmentConfig{
				APIServer: "https://custom.example.com/api/v3",
				Env:       "custom",
			},
		},
		{
			name:      "custom url overrides dev",
			tunnel:    false,
			local:     false,
			dev:       true,
			test:      false,
			scicatUrl: "https://custom.example.com/api/v3",
			want: EnvironmentConfig{
				APIServer: "https://custom.example.com/api/v3",
				Env:       "custom",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ConfigureEnvironment(tt.tunnel, tt.local, tt.dev, tt.test, tt.scicatUrl)
			if got.APIServer != tt.want.APIServer || got.Env != tt.want.Env {
				t.Errorf("ConfigureEnvironment() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestConfigureArchiveEnvironment(t *testing.T) {
	tests := []struct {
		name      string
		tunnel    bool
		local     bool
		dev       bool
		test      bool
		scicatUrl string
		rsyncUrl  string
		want      ArchiveConfig
	}{
		{
			name:      "production default",
			tunnel:    false,
			local:     false,
			dev:       false,
			test:      false,
			scicatUrl: "",
			rsyncUrl:  "",
			want: ArchiveConfig{
				APIServer:   PROD_API_SERVER,
				RSYNCServer: PROD_RSYNC_ARCHIVE_SERVER,
				Env:         "production",
			},
		},
		{
			name:      "dev environment",
			tunnel:    false,
			local:     false,
			dev:       true,
			test:      false,
			scicatUrl: "",
			rsyncUrl:  "",
			want: ArchiveConfig{
				APIServer:   DEV_API_SERVER,
				RSYNCServer: DEV_RSYNC_ARCHIVE_SERVER,
				Env:         "dev",
			},
		},
		{
			name:      "test environment",
			tunnel:    false,
			local:     false,
			dev:       false,
			test:      true,
			scicatUrl: "",
			rsyncUrl:  "",
			want: ArchiveConfig{
				APIServer:   TEST_API_SERVER,
				RSYNCServer: TEST_RSYNC_ARCHIVE_SERVER,
				Env:         "test",
			},
		},
		{
			name:      "local environment",
			tunnel:    false,
			local:     true,
			dev:       false,
			test:      false,
			scicatUrl: "",
			rsyncUrl:  "",
			want: ArchiveConfig{
				APIServer:   LOCAL_API_SERVER,
				RSYNCServer: LOCAL_RSYNC_ARCHIVE_SERVER,
				Env:         "local",
			},
		},
		{
			name:      "tunnel environment",
			tunnel:    true,
			local:     false,
			dev:       false,
			test:      false,
			scicatUrl: "",
			rsyncUrl:  "",
			want: ArchiveConfig{
				APIServer:   TUNNEL_API_SERVER,
				RSYNCServer: TUNNEL_RSYNC_ARCHIVE_SERVER,
				Env:         "dev",
			},
		},
		{
			name:      "custom url without rsync",
			tunnel:    false,
			local:     false,
			dev:       false,
			test:      false,
			scicatUrl: "https://custom.example.com/api/v3",
			rsyncUrl:  "",
			want: ArchiveConfig{
				APIServer:   "https://custom.example.com/api/v3",
				RSYNCServer: PROD_RSYNC_ARCHIVE_SERVER,
				Env:         "custom-production",
			},
		},
		{
			name:      "custom url with rsync",
			tunnel:    false,
			local:     false,
			dev:       false,
			test:      false,
			scicatUrl: "https://custom.example.com/api/v3",
			rsyncUrl:  "custom-rsync.example.com",
			want: ArchiveConfig{
				APIServer:   "https://custom.example.com/api/v3",
				RSYNCServer: "custom-rsync.example.com",
				Env:         "custom",
			},
		},
		{
			name:      "custom url overrides dev with rsync",
			tunnel:    false,
			local:     false,
			dev:       true,
			test:      false,
			scicatUrl: "https://custom.example.com/api/v3",
			rsyncUrl:  "custom-rsync.example.com",
			want: ArchiveConfig{
				APIServer:   "https://custom.example.com/api/v3",
				RSYNCServer: "custom-rsync.example.com",
				Env:         "custom",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ConfigureArchiveEnvironment(tt.tunnel, tt.local, tt.dev, tt.test, tt.scicatUrl, tt.rsyncUrl)
			if got.APIServer != tt.want.APIServer || got.RSYNCServer != tt.want.RSYNCServer || got.Env != tt.want.Env {
				t.Errorf("ConfigureArchiveEnvironment() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestConfigureRetrieveEnvironment(t *testing.T) {
	tests := []struct {
		name      string
		tunnel    bool
		local     bool
		dev       bool
		test      bool
		scicatUrl string
		rsyncUrl  string
		want      RetrieveConfig
	}{
		{
			name:      "production default",
			tunnel:    false,
			local:     false,
			dev:       false,
			test:      false,
			scicatUrl: "",
			rsyncUrl:  "",
			want: RetrieveConfig{
				APIServer:   PROD_API_SERVER,
				RSYNCServer: PROD_RSYNC_RETRIEVE_SERVER,
				Env:         "production",
			},
		},
		{
			name:      "dev environment",
			tunnel:    false,
			local:     false,
			dev:       true,
			test:      false,
			scicatUrl: "",
			rsyncUrl:  "",
			want: RetrieveConfig{
				APIServer:   DEV_API_SERVER,
				RSYNCServer: DEV_RSYNC_RETRIEVE_SERVER,
				Env:         "dev",
			},
		},
		{
			name:      "test environment",
			tunnel:    false,
			local:     false,
			dev:       false,
			test:      true,
			scicatUrl: "",
			rsyncUrl:  "",
			want: RetrieveConfig{
				APIServer:   TEST_API_SERVER,
				RSYNCServer: TEST_RSYNC_RETRIEVE_SERVER,
				Env:         "test",
			},
		},
		{
			name:      "local environment",
			tunnel:    false,
			local:     true,
			dev:       false,
			test:      false,
			scicatUrl: "",
			rsyncUrl:  "",
			want: RetrieveConfig{
				APIServer:   LOCAL_API_SERVER,
				RSYNCServer: LOCAL_RSYNC_RETRIEVE_SERVER,
				Env:         "local",
			},
		},
		{
			name:      "custom url without rsync",
			tunnel:    false,
			local:     false,
			dev:       false,
			test:      false,
			scicatUrl: "https://custom.example.com/api/v3",
			rsyncUrl:  "",
			want: RetrieveConfig{
				APIServer:   "https://custom.example.com/api/v3",
				RSYNCServer: PROD_RSYNC_RETRIEVE_SERVER,
				Env:         "custom-production",
			},
		},
		{
			name:      "custom url with rsync",
			tunnel:    false,
			local:     false,
			dev:       false,
			test:      false,
			scicatUrl: "https://custom.example.com/api/v3",
			rsyncUrl:  "custom-rsync.example.com",
			want: RetrieveConfig{
				APIServer:   "https://custom.example.com/api/v3",
				RSYNCServer: "custom-rsync.example.com",
				Env:         "custom",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ConfigureRetrieveEnvironment(tt.tunnel, tt.local, tt.dev, tt.test, tt.scicatUrl, tt.rsyncUrl)
			if got.APIServer != tt.want.APIServer || got.RSYNCServer != tt.want.RSYNCServer || got.Env != tt.want.Env {
				t.Errorf("ConfigureRetrieveEnvironment() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
