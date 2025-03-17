package cliutils

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"runtime"
	"sync"
)

func GetScicatToken(scicatUrl string) string {
	semaphore := sync.Mutex{}

	fmt.Printf("You must login using your credentials in the browser!")
	var scicatToken string

	mux := http.NewServeMux()
	srv := http.Server{
		Addr:    "localhost:8080",
		Handler: mux,
	}
	mux.HandleFunc("/success", func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("Success url was called... ")
		q := r.URL.Query()
		scicatToken = q.Get("access-token")
		w.WriteHeader(200)
		w.Write(nil)

		shutdown := func() {
			semaphore.Unlock()
			srv.Shutdown(context.Background())
		}
		go shutdown()
	})

	semaphore.Lock()
	go srv.ListenAndServe()
	openBrowser(scicatUrl)
	semaphore.Lock()

	if scicatToken == "" {
		log.Fatal("Failed to receive the token. Exiting...")
	}

	fmt.Println("Token received OK")

	return scicatToken
}

func openBrowser(url string) error {
	var err error
	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	return err
}
