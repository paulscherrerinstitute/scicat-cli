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
	client, err := ssh.Dial("tcp", server, &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		// HostKeyCallback: trustedHostKeyCallback("ssh-rsa-cert-v01@openssh.com AAAAHHNzaC1yc2EtY2VydC12MDFAb3BlbnNzaC5jb20AAAAgpvj6p0hDZj5gBn6tURDaMiJYKOl/XYvX2Dk9LWo7C6QAAAADAQABAAABAQCxRJJcF9Z4CnUq2KZ/ODFi9NCYgKgpN60FRQI7XUIcvshVhs8cSaWZUyciWVKIY4JyjVztjnGjTUcTj+oHN6IzmCb85ZNtyDnn9q7bMyhKIXkFMUVKULz8yFL34odYqXPHRxJzC6XWIn28CYpFjXueHJUrpfgnu4L0lqNb3JAKOIuU1unM0skuxd0n6h423bc5NPLrDySi2PVKAENO0+pyOO3ktxhvQpvDgOf5A4HztkE4I8dHkEPVcbOr2T2EVrbGiVgFghyDq5bgFFobC4E8fae9KOdvkeHAJSfgH/VE4ydzougq3fiMD5TK0p0uQvPngj85LONCb3LFmUqEalRNAAAAAAAAAAAAAAACAAAAE2FyZW1hdGVzdDJpbi5wc2kuY2gAAAAXAAAAE2FyZW1hdGVzdDJpbi5wc2kuY2gAAAAAAAAAAP//////////AAAAAAAAAAAAAAAAAAABFwAAAAdzc2gtcnNhAAAAAwEAAQAAAQEAzNx0DarU7ZlbZY3KOM13yjGXP+i7nfvFTB617qAf93tPk1QSoUvztqQGdR1NKN4relU3X/qOuofBZhiglA4sHAz7kPwqpqNfJ1sIC2HnSsR2DO6GYqfvbbNCA7KNi3m27LKRqxOFejaop3WfjAJCnt1L5yVBUZ5PufnkmLAnuCIYnVNNnlRPiJ1z5AqXqbjuBlYs4ld9eUBwQag3l370LQPxD8NUyyy1b4DaJDCk3M+zmy2zuolyitexQKrft6M38zleoRkSOINuNPb1va2I4WRVCiYhtFRpqil+73mblGiJ2lSwKjx1AE/st4fxNKAjbWPIo12inrhEY+QSRPhGmwAAAQ8AAAAHc3NoLXJzYQAAAQANcNiIvoMy5JRjEcsIRcALUNt82AZQ3PrBrJ72NvSKpbzfPbHCscgj1R+n7dfzFfoavGDzlu2JWtZAIn0MahNDnSzjSyW3/gCvFTXYh4uaDvEX9S1tXkPrm2oLbqUINf8zrlhTkWlBfS/DlkyLh2nRcPQo6cvQ+KMB4vy5afcdd9aHUPX1jBJK5JiYiTYxD733MWBxEnAKdCgGRB9oQHM6DXNAQlvHzSLy9LnHP9IB8bXRt47I+ilL3UgYz3qRZsRkXZyf9vZhWPhcdoAV0IoyalpVxfIsxWlve3QqlNb6Y0bsjxk8XZ+Ne1J08N/HxHyjj93dEOB4Pb7fRprslp3H"),
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
