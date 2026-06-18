package backend

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

type AuthOptions struct {
	User        string
	Token       string
	Oidc        bool
	TestEnv     bool
	AutoArchive bool
}

type TransportEngine struct {
	Client      *http.Client
	APIServer   string
	RsyncServer string
	Scanner     *bufio.Scanner
}

type UserSession struct {
	User         map[string]string
	AccessGroups []string
}

func BootstrapTransportEngine(apiServer, rsyncServer string) *TransportEngine {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: false},
		},
		Timeout: 120 * time.Second,
	}

	return &TransportEngine{
		Client:      client,
		APIServer:   apiServer,
		RsyncServer: rsyncServer,
		Scanner:     bufio.NewScanner(os.Stdin),
	}
}

func (t *TransportEngine) EnforceVersionGuardrail(currentVersion string) {
	const CMD_NAME = "datasetIngestor"
	datasetUtils.CheckForNewVersion(t.Client, CMD_NAME, currentVersion)
}

func (t *TransportEngine) VerifyServiceAvailability(testEnv, autoArchive bool) {
	datasetUtils.CheckForServiceAvailability(t.Client, testEnv, autoArchive)
}

func (t *TransportEngine) ExecuteAuthenticationChallenge(opts AuthOptions) (*UserSession, error) {
	user, accessGroups, err := cliutils.Authenticate(cliutils.RealAuthenticator{}, t.Client, t.APIServer, opts.User, opts.Token, opts.Oidc)
	if err != nil {
		return nil, fmt.Errorf("session authorization rejected: %w", err)
	}

	return &UserSession{
		User:         user,
		AccessGroups: accessGroups,
	}, nil
}

func (t *TransportEngine) InitializeSession(currentVersion string, opts AuthOptions) (*UserSession, error) {
	t.EnforceVersionGuardrail(currentVersion)
	t.VerifyServiceAvailability(opts.TestEnv, opts.AutoArchive)
	return t.ExecuteAuthenticationChallenge(opts)
}
