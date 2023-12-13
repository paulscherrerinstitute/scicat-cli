// +build aix darwin dragonfly freebsd js,wasm linux nacl netbsd openbsd solaris

// very important: there must be an empty line after the build flag line .
package datasetUtils

import (
	"log"
	"os"
	"os/exec"
	"time"

	expect "github.com/Netflix/go-expect"
)

func RunKinit(username string, password string) (err error) {

	c, err := expect.NewConsole(expect.WithStdout(os.Stdout))
	if err != nil {
		log.Printf("Warning: Could not start kinit to get Kerberos tickets: %v\n", err)
	}
	defer c.Close()

	cmd := exec.Command("/usr/bin/kinit", username)
	cmd.Stdin = c.Tty()
	cmd.Stdout = c.Tty()
	cmd.Stderr = c.Tty()

	err = cmd.Start()
	if err != nil {
		log.Printf("Warning: Could not start kinit to get Kerberos tickets: %v\n", err)
	} else {
		time.Sleep(time.Second)
		c.SendLine(password)

		err = cmd.Wait()
		if err != nil {
			log.Printf("Warning: kinit error: %v\n", err)
		}
	}
	return err
}
