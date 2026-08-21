package main

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunWaitCopiesSocketBytesToStdout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		conn.Write([]byte("hello\n"))
		conn.Close()
	}()

	var stdout, stderr bytes.Buffer
	code := runWait(path, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d (stderr: %s)", code, stderr.String())
	}
	if stdout.String() != "hello\n" {
		t.Fatalf("got %q", stdout.String())
	}
}

func TestRunWaitReturnsErrorOnMissingSocket(t *testing.T) {
	path := filepath.Join(os.TempDir(), "does-not-exist-"+time.Now().Format("150405")+".sock")
	var stdout, stderr bytes.Buffer
	code := runWait(path, &stdout, &stderr)
	if code == 0 {
		t.Fatal("expected non-zero exit code for a missing socket")
	}
	if stderr.Len() == 0 {
		t.Fatal("expected an error message on stderr")
	}
}
