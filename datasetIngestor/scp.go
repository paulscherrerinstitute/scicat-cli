/*
This package provides simple SCP client for copying data recursively to remote server. It's built
on top of x/crypto/ssh. Code from aedavelli.

*/
package datasetIngestor

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bodgit/sshkrb5"
	"github.com/kballard/go-shellquote"
	"golang.org/x/crypto/ssh"
)

type Client struct {
	SshClient    *ssh.Client
	PreseveTimes bool
	Quiet        bool
}

// create human-readable SSH-key strings
func keyString(k ssh.PublicKey) string {
	return k.Type() + " " + base64.StdEncoding.EncodeToString(k.Marshal()) // e.g. "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTY...."
}

/* trustedHostKeyCallback returns a function that serves as a callback for SSH host key verification.
If a trustedKey is provided, the callback will verify if the key from the server matches the trustedKey.
If they don't match, it returns an error.
If no trustedKey is provided, the callback will log a warning that SSH-key verification is not in effect,
but it will not stop the connection.
Parameters:
trustedKey: A string representation of the trusted SSH public key.
Returns:
An ssh.HostKeyCallback function for SSH host key verification. */
func trustedHostKeyCallback(trustedKey string) ssh.HostKeyCallback {
	trustedKey = strings.TrimSpace(trustedKey)
	if trustedKey == "" {
		return func(_ string, _ net.Addr, k ssh.PublicKey) error {
			log.Printf("WARNING: SSH-key verification is *NOT* in effect: to fix, add this trustedKey: %q", keyString(k))
			return nil
		}
	}

	return func(_ string, _ net.Addr, k ssh.PublicKey) error {
		ks := keyString(k)
		ks = strings.TrimSpace(ks)		
		if trustedKey != ks {
			return fmt.Errorf("SSH-key verification: expected %q but got %q", trustedKey, ks)
		}

		return nil
	}
}

// Form send command based on client configuration
func (c *Client) getSendCommand(dst string) string {
	cmd := "scp -rt"

	if c.PreseveTimes {
		cmd += "p"
	}

	if c.Quiet {
		cmd += "q"
	}

	return fmt.Sprintf("%s %s", cmd, shellquote.Join(dst))
	// return fmt.Sprintf("%s \"%s\"", cmd, dst)
}

// Send the files dst directory on remote side. The paths can be regular files or directories.
func (c *Client) Send(dst string, paths ...string) error {
	// Create an SSH session
	session, err := c.SshClient.NewSession()
	if err != nil {
		return errors.New("Failed to create SSH session: " + err.Error())
	}
	defer session.Close()

	// Setup Input strem
	w, err := session.StdinPipe()
	if err != nil {
		return errors.New("Unable to get stdin: " + err.Error())
	}
	defer w.Close()

	// Setup Output strem
	r, err := session.StdoutPipe()
	if err != nil {
		return errors.New("Unable to get Stdout: " + err.Error())
	}

	fmt.Println("Sendcommand:", c.getSendCommand(dst))
	if err := session.Start(c.getSendCommand(dst)); err != nil {
		return errors.New("Failed to start: " + err.Error())
	}

	errors := make(chan error)

	go func() {
		errors <- session.Wait()
	}()

	for _, p := range paths {
		if err := c.walkAndSend(w, p); err != nil {
			return err
		}
	}
	w.Close()
	io.Copy(os.Stdout, r)
	<-errors

	return nil
}

// send regular file
func (c *Client) sendRegularFile(w io.Writer, path string, fi os.FileInfo) error {
	if c.PreseveTimes {
		_, err := fmt.Fprintf(w, "T%d 0 %d 0\n", fi.ModTime().Unix(), time.Now().Unix())
		if err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, "C%#o %d %s\n", fi.Mode().Perm(), fi.Size(), fi.Name())
	if err != nil {
		return errors.New("Copy failed: " + err.Error())
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	io.Copy(w, f)
	fmt.Fprint(w, "\x00")
	if !c.Quiet {
		fmt.Println("Copied: ", path)
	}
	return nil
}

// Walk and Send directory
/* walkAndSend recursively walks through the directory specified by 'src', 
and sends each file it encounters to the writer 'w'. 
If 'src' is a regular file, it sends the file directly. 
If 'src' is a directory, it walks through the directory and sends each file it encounters.
It also sends directory change commands (push and pop) to the writer.
If 'c.PreseveTimes' is true, it sends the modification time of each file and directory to the writer.
It returns an error if any operation fails. */
func (c *Client) walkAndSend(w io.Writer, src string) error {
	cleanedPath := filepath.Clean(src)

	fi, err := os.Stat(cleanedPath)
	if err != nil {
		return err
	}

	if fi.Mode().IsRegular() {
		if err = c.sendRegularFile(w, cleanedPath, fi); err != nil {
			return err
		}
	} else {
		// It is a directory need to walk and copy
		dirStack := strings.Split(cleanedPath, fmt.Sprintf("%c", os.PathSeparator))
		startStackLen := len(dirStack)
		dirStack = dirStack[:startStackLen-1]
		startStackLen--
		err = filepath.Walk(cleanedPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			tmpDirStack := strings.Split(path, fmt.Sprintf("%c", os.PathSeparator))
			i, di, ci := 0, 0, 0
			dl, cl := len(dirStack), len(tmpDirStack)

			if info.Mode().IsRegular() {
				tmpDirStack = tmpDirStack[:cl-1]
				cl--
			}

			for i = 0; i < dl && i < cl; i++ {
				if dirStack[i] != tmpDirStack[i] {
					break
				}
				di++
				ci++
			}

			for di < dl { // We need to pop
				fmt.Fprintf(w, "E\n")
				di++
			}

			for ci < cl { // We need to push
				if c.PreseveTimes {
					_, err := fmt.Fprintf(w, "T%d 0 %d 0\n", info.ModTime().Unix(), time.Now().Unix())
					if err != nil {
						return err
					}
				}
				fmt.Fprintf(w, "D%#o 0 %s\n", info.Mode().Perm(), tmpDirStack[ci])
				ci++
			}

			dirStack = tmpDirStack
			if info.Mode().IsRegular() {
				if err = c.sendRegularFile(w, path, info); err != nil {
					return err
				}
			}
			return nil
		})

		if err != nil {
			return err
		}

		dl := len(dirStack) - 1

		for dl >= startStackLen {
			fmt.Fprintf(w, "E\n")
			dl--
		}
	}
	return nil
}

// Creates a new SCP client.  It enables preserve time stamps
func NewDumbClient(username, password, server string) (*Client, error) {
	// Extract host for GSSAPI (e.g., server "name:22" -> "name")
	host, _, err := net.SplitHostPort(server)
	if err != nil {
		host = server
	}

	var authMethods []ssh.AuthMethod

	// Try to initialize Kerberos/GSSAPI using the OS ticket cache
	gssClient, gssErr := sshkrb5.NewClient()
	if gssErr == nil && gssClient != nil {
		authMethods = append(authMethods, ssh.GSSAPIWithMICAuthMethod(gssClient, host))
	}

	// Always include Password as a fallback
	if password != "" {
		authMethods = append(authMethods, ssh.Password(password))
	}

	// Dial the server
	client, err := ssh.Dial("tcp", server, &ssh.ClientConfig{
		User:            username,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         15 * time.Second,
	})

	if err != nil {
		return nil, err
	}

	return &Client{
		SshClient:    client,
		PreseveTimes: true,
	}, nil
}

// Creates a new SCP client form ssh.Client and preserve time stamps
func NewClient(c *ssh.Client, pt bool) *Client {
	return &Client{
		SshClient:    c,
		PreseveTimes: pt,
	}
}
