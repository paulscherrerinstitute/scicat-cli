package cliutils

import (
	"context"
	"fmt"
	"os"

	"github.com/SwissOpenEM/globus"
	"golang.org/x/oauth2"
	"gopkg.in/yaml.v2"
)

type GlobusConfig struct {
	ClientID              string   `yaml:"client-id"`
	ClientSecret          string   `yaml:"client-secret,omitempty"`
	RedirectURL           string   `yaml:"redirect-url"`
	Scopes                []string `yaml:"scopes,omitempty"`
	SourceCollection      string   `yaml:"source-collection"`
	SourcePrefixPath      string   `yaml:"source-prefix-path,omitempty"`
	DestinationCollection string   `yaml:"destination-collection"`
	DestinationPrefixPath string   `yaml:"destination-prefix-path,omitempty"`
}

func GlobusLogin(confPath string) (gClient globus.GlobusClient, gConfig GlobusConfig, err error) {
	// read in config
	data, err := os.ReadFile(confPath)
	if err != nil {
		return globus.GlobusClient{}, GlobusConfig{}, fmt.Errorf("can't read globus config: %v", err)
	}
	err = yaml.Unmarshal(data, &gConfig)
	if err != nil {
		return globus.GlobusClient{}, GlobusConfig{}, fmt.Errorf("can't unmarshal globus config: %v", err)
	}

	// config setup
	ctx := context.Background()
	clientConfig := globus.AuthGenerateOauthClientConfig(ctx, gConfig.ClientID, gConfig.ClientSecret, gConfig.RedirectURL, gConfig.Scopes)
	verifier := oauth2.GenerateVerifier()
	clientConfig.AuthCodeURL("state", oauth2.AccessTypeOffline, oauth2.S256ChallengeOption(verifier))

	// redirect user to consent page to ask for permission and obtain the code
	url := clientConfig.AuthCodeURL("state", oauth2.AccessTypeOffline, oauth2.S256ChallengeOption(verifier))
	fmt.Printf("Visit the URL for the auth dialog: %v\n\nEnter the received code here: ", url)

	// negotiate token and create client
	var code string
	if _, err := fmt.Scan(&code); err != nil {
		return globus.GlobusClient{}, GlobusConfig{}, err
	}
	tok, err := clientConfig.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		return globus.GlobusClient{}, GlobusConfig{}, fmt.Errorf("oauth2 exchange failed: %v", err)
	}

	// return globus client
	return globus.HttpClientToGlobusClient(clientConfig.Client(ctx, tok)), gConfig, nil
}
