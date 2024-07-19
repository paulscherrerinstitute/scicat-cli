package datasetUtils

import (
	"net/http"
)

// An interface with the methods so that we can mock them in tests
type Authenticator interface {
	AuthenticateUser(httpClient *http.Client, APIServer string, username string, password string) (map[string]string, []string)
	GetUserInfoFromToken(httpClient *http.Client, APIServer string, token string) (map[string]string, []string)
}

type RealAuthenticator struct{}

func (r *RealAuthenticator) AuthenticateUser(httpClient *http.Client, APIServer string, username string, password string) (map[string]string, []string) {
	return AuthenticateUser(httpClient, APIServer, username, password)
}

func (r *RealAuthenticator) GetUserInfoFromToken(httpClient *http.Client, APIServer string, token string) (map[string]string, []string) {
	return GetUserInfoFromToken(httpClient, APIServer, token)
}
