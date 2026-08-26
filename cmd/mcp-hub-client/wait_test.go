package main

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/waiter"
)

func TestRunWaitSendsOnceModeByteAndCopiesSocketBytesToStdout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	modeCh := make(chan byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		mode := make([]byte, 1)
		conn.Read(mode)
		modeCh <- mode[0]
		conn.Write([]byte("hello\n"))
		conn.Close()
	}()

	var stdout, stderr bytes.Buffer
	code := runWait(path, false, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d (stderr: %s)", code, stderr.String())
	}
	if stdout.String() != "hello\n" {
		t.Fatalf("got %q", stdout.String())
	}
	if got := <-modeCh; got != waiter.ModeOnce {
		t.Fatalf("expected ModeOnce byte, got %v", got)
	}
}

func TestRunWaitFollowSendsFollowModeByte(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	modeCh := make(chan byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		mode := make([]byte, 1)
		conn.Read(mode)
		modeCh <- mode[0]
		conn.Write([]byte("chunk one\n\n"))
		conn.Close() // end the stream so runWait returns for the test
	}()

	var stdout, stderr bytes.Buffer
	code := runWait(path, true, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d (stderr: %s)", code, stderr.String())
	}
	if stdout.String() != "chunk one\n\n" {
		t.Fatalf("got %q", stdout.String())
	}
	if got := <-modeCh; got != waiter.ModeFollow {
		t.Fatalf("expected ModeFollow byte, got %v", got)
	}
}

func TestRunWaitReturnsErrorOnMissingSocket(t *testing.T) {
	path := filepath.Join(os.TempDir(), "does-not-exist-"+time.Now().Format("150405")+".sock")
	var stdout, stderr bytes.Buffer
	code := runWait(path, false, &stdout, &stderr)
	if code == 0 {
		t.Fatal("expected non-zero exit code for a missing socket")
	}
	if stderr.Len() == 0 {
		t.Fatal("expected an error message on stderr")
	}
}
