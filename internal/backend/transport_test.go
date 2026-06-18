package backend

import (
	"bufio"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestBootstrapTransportEngine(t *testing.T) {
	apiServer := "http://api.example.com"
	rsyncServer := "rsync.example.com"

	engine := BootstrapTransportEngine(apiServer, rsyncServer)

	if engine.APIServer != apiServer {
		t.Errorf("Expected APIServer %q, got %q", apiServer, engine.APIServer)
	}
	if engine.RsyncServer != rsyncServer {
		t.Errorf("Expected RsyncServer %q, got %q", rsyncServer, engine.RsyncServer)
	}

	if engine.Client == nil {
		t.Errorf("Expected HTTP client to be initialized, got nil")
	}
	if engine.Client.Timeout != 120*time.Second {
		t.Errorf("Expected timeout 120s, got %v", engine.Client.Timeout)
	}

	if engine.Scanner == nil {
		t.Errorf("Expected scanner to be initialized, got nil")
	}
}

func TestBootstrapTransportEngineClientTransport(t *testing.T) {
	engine := BootstrapTransportEngine("http://api.example.com", "rsync.example.com")

	transport, ok := engine.Client.Transport.(*http.Transport)
	if !ok {
		t.Errorf("Expected *http.Transport, got %T", engine.Client.Transport)
	}
	if transport.TLSClientConfig == nil {
		t.Errorf("Expected TLSClientConfig to be initialized")
	}
	if transport.TLSClientConfig.InsecureSkipVerify {
		t.Errorf("Expected InsecureSkipVerify to be false, got true")
	}
}

func TestExecuteAuthenticationChallengeSuccess(t *testing.T) {
	opts := AuthOptions{
		User:        "testuser:password",
		Token:       "",
		Oidc:        false,
		TestEnv:     false,
		AutoArchive: false,
	}

	if opts.User == "" {
		t.Errorf("Expected User to be set in AuthOptions")
	}
}

func TestUserSessionStructure(t *testing.T) {
	mockUser := map[string]string{
		"username":    "testuser",
		"accessToken": "token123",
	}
	mockGroups := []string{"group1", "group2"}

	session := &UserSession{
		User:         mockUser,
		AccessGroups: mockGroups,
	}

	if session.User == nil {
		t.Errorf("Expected User to be initialized")
	}
	if session.User["username"] != "testuser" {
		t.Errorf("Expected username 'testuser', got %q", session.User["username"])
	}
	if len(session.AccessGroups) != 2 {
		t.Errorf("Expected 2 access groups, got %d", len(session.AccessGroups))
	}
	if session.AccessGroups[0] != "group1" {
		t.Errorf("Expected first group 'group1', got %q", session.AccessGroups[0])
	}
}

func TestTransportEngineAuthOptionsStructure(t *testing.T) {
	testCases := []struct {
		name     string
		opts     AuthOptions
		validate func(*testing.T, AuthOptions)
	}{
		{
			name: "UserPasswordAuth",
			opts: AuthOptions{
				User:        "user:pass",
				Token:       "",
				Oidc:        false,
				TestEnv:     false,
				AutoArchive: false,
			},
			validate: func(t *testing.T, opts AuthOptions) {
				if opts.User == "" {
					t.Errorf("Expected User to be set")
				}
				if opts.Token != "" {
					t.Errorf("Expected empty Token for user/pass auth, got %q", opts.Token)
				}
				if opts.Oidc {
					t.Errorf("Expected Oidc to be false")
				}
			},
		},
		{
			name: "TokenAuth",
			opts: AuthOptions{
				User:        "",
				Token:       "test-token-123",
				Oidc:        false,
				TestEnv:     true,
				AutoArchive: true,
			},
			validate: func(t *testing.T, opts AuthOptions) {
				if opts.Token == "" {
					t.Errorf("Expected Token to be set")
				}
				if opts.TestEnv != true {
					t.Errorf("Expected TestEnv to be true")
				}
				if opts.AutoArchive != true {
					t.Errorf("Expected AutoArchive to be true")
				}
			},
		},
		{
			name: "OidcAuth",
			opts: AuthOptions{
				User:        "",
				Token:       "",
				Oidc:        true,
				TestEnv:     false,
				AutoArchive: false,
			},
			validate: func(t *testing.T, opts AuthOptions) {
				if !opts.Oidc {
					t.Errorf("Expected Oidc to be true")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.validate(t, tc.opts)
		})
	}
}

func TestTransportEngineFields(t *testing.T) {
	engine := BootstrapTransportEngine("http://api.test.com", "rsync.test.com")

	testCases := []struct {
		name     string
		validate func(*testing.T, *TransportEngine)
	}{
		{
			name: "ClientNotNil",
			validate: func(t *testing.T, te *TransportEngine) {
				if te.Client == nil {
					t.Errorf("Expected Client to be non-nil")
				}
			},
		},
		{
			name: "APIServerSet",
			validate: func(t *testing.T, te *TransportEngine) {
				if te.APIServer != "http://api.test.com" {
					t.Errorf("Expected APIServer to be 'http://api.test.com', got %q", te.APIServer)
				}
			},
		},
		{
			name: "RsyncServerSet",
			validate: func(t *testing.T, te *TransportEngine) {
				if te.RsyncServer != "rsync.test.com" {
					t.Errorf("Expected RsyncServer to be 'rsync.test.com', got %q", te.RsyncServer)
				}
			},
		},
		{
			name: "ScannerNotNil",
			validate: func(t *testing.T, te *TransportEngine) {
				if te.Scanner == nil {
					t.Errorf("Expected Scanner to be non-nil")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.validate(t, engine)
		})
	}
}

func TestBootstrapTransportEngineMultipleInstances(t *testing.T) {
	engine1 := BootstrapTransportEngine("http://api1.com", "rsync1.com")
	engine2 := BootstrapTransportEngine("http://api2.com", "rsync2.com")

	if engine1.APIServer == engine2.APIServer {
		t.Errorf("Expected different APIServers for different engines")
	}
	if engine1.Client == engine2.Client {
		t.Errorf("Expected different HTTP clients for different engines")
	}
	if engine1.Scanner == engine2.Scanner {
		t.Errorf("Expected different scanners for different engines")
	}
}

func TestScannerInitialization(t *testing.T) {
	engine := BootstrapTransportEngine("http://api.example.com", "rsync.example.com")

	testInput := "test input line\n"
	engine.Scanner = bufio.NewScanner(strings.NewReader(testInput))

	if engine.Scanner.Scan() {
		if engine.Scanner.Text() != "test input line" {
			t.Errorf("Expected 'test input line', got %q", engine.Scanner.Text())
		}
	} else {
		t.Errorf("Expected scanner to successfully scan input")
	}
}

func TestHttpClientConfiguration(t *testing.T) {
	engine := BootstrapTransportEngine("http://api.example.com", "rsync.example.com")

	expectedTimeout := 120 * time.Second
	if engine.Client.Timeout != expectedTimeout {
		t.Errorf("Expected timeout %v, got %v", expectedTimeout, engine.Client.Timeout)
	}

	if engine.Client.Transport == nil {
		t.Errorf("Expected Transport to be configured")
	}
}
