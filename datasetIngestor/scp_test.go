package datasetIngestor

import (
	"testing"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"golang.org/x/crypto/ssh"
	"bytes"
	"os"
	"fmt"
)

// Checks if the function returns an error when the provided key does not match the trusted key.
func TestTrustedHostKeyCallback(t *testing.T) {
	// Generate a test key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	
	publicKeyRsa, err := ssh.NewPublicKey(&privateKey.PublicKey)
	if err != nil {
		t.Fatalf("Failed to generate public key: %v", err)
	}
	
	publicKeyBytes := ssh.MarshalAuthorizedKey(publicKeyRsa)
	trustedKey := string(publicKeyBytes)
	
	// Create the callback
	callback := trustedHostKeyCallback(trustedKey)
	
	// Test the callback with the correct key
	err = callback("", nil, publicKeyRsa)
	if err != nil {
		t.Errorf("Expected no error for correct key, got: %v", err)
	}
	
	// Generate a different key for testing mismatch
	privateKey2, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	
	publicKeyRsa2, err := ssh.NewPublicKey(&privateKey2.PublicKey)
	if err != nil {
		t.Fatalf("Failed to generate public key: %v", err)
	}
	
	// Test the callback with a different key
	err = callback("", nil, publicKeyRsa2)
	if err == nil {
		t.Errorf("Expected error for incorrect key, got nil")
	}
}

func generateTestKeyPair(bits int) (string, string, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return "", "", err
	}
	
	privateKeyDer := x509.MarshalPKCS1PrivateKey(privateKey)
	privateKeyBlock := pem.Block{
		Type:    "RSA PRIVATE KEY",
		Headers: nil,
		Bytes:   privateKeyDer,
	}
	privateKeyPem := string(pem.EncodeToMemory(&privateKeyBlock))
	
	publicKeyRsa, err := ssh.NewPublicKey(&privateKey.PublicKey)
	if err != nil {
		return "", "", err
	}
	publicKeyBytes := ssh.MarshalAuthorizedKey(publicKeyRsa)
	publicKeyPem := string(publicKeyBytes)
	
	return privateKeyPem, publicKeyPem, nil
}

func TestGetSendCommand(t *testing.T) {
	client := &Client{
		PreseveTimes: true,
		Quiet:        true,
	}
	
	dst := "/path/to/destination"
	expected := "scp -rtpq /path/to/destination"
	
	result := client.getSendCommand(dst)
	
	if result != expected {
		t.Errorf("getSendCommand() = %s; want %s", result, expected)
	}
}

// Checks if the function correctly sends a regular file and handles errors properly
func TestSendRegularFile(t *testing.T) {
	client := &Client{
		PreseveTimes: true,
		Quiet:        true,
	}
	
	// Create a temporary file for testing
	tmpfile, err := os.CreateTemp("", "example")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name()) // clean up
	
	// Write some data to the file
	text := []byte("This is a test file.")
	if _, err := tmpfile.Write(text); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}
	
	// Get file info
	fi, err := os.Stat(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	
	// Create a buffer to use as the writer
	var buf bytes.Buffer
	
	// Call the function
	err = client.sendRegularFile(&buf, tmpfile.Name(), fi)
	if err != nil {
		t.Errorf("sendRegularFile() error = %v", err)
	}
	
	// Check if the file content was written to the buffer
	if !bytes.Contains(buf.Bytes(), text) {
		t.Errorf("sendRegularFile() did not write file content to writer")
	}
	
	// Check if the file permissions were written to the buffer
	perm := fmt.Sprintf("C%04o", fi.Mode().Perm())
	if !bytes.Contains(buf.Bytes(), []byte(perm)) {
		t.Errorf("sendRegularFile() did not write file permissions to writer")
	}
}

// Checks if the function returns an error, if the file content was written to the buffer, and if the directory change commands were written to the buffer
func TestWalkAndSend(t *testing.T) {
	client := &Client{
		PreseveTimes: true,
		Quiet:        true,
	}
	
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "example")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir) // clean up
	
	// Create a file in the temporary directory
	tmpfile, err := os.CreateTemp(tmpDir, "file")
	if err != nil {
		t.Fatal(err)
	}
	text := []byte("This is a test file.")
	if _, err := tmpfile.Write(text); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}
	
	// Create a buffer to use as the writer
	var buf bytes.Buffer
	
	// Call the function
	err = client.walkAndSend(&buf, tmpDir)
	if err != nil {
		t.Errorf("walkAndSend() error = %v", err)
	}
	
	// Check if the file content was written to the buffer
	if !bytes.Contains(buf.Bytes(), text) {
		t.Errorf("walkAndSend() did not write file content to writer")
	}
	
	// Check if the directory change commands were written to the buffer
	if !bytes.Contains(buf.Bytes(), []byte("D")) || !bytes.Contains(buf.Bytes(), []byte("E")) {
		t.Errorf("walkAndSend() did not write directory change commands to writer")
	}
}

