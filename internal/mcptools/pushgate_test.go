package mcptools

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/secforge/harness-transport/deliver"
)

type recordingPusher struct {
	mu    sync.Mutex
	texts []string
}

func (p *recordingPusher) Push(cursor, body string, more bool) (deliver.Receipt, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.texts = append(p.texts, body)
	return deliver.Receipt{}, nil
}

func (p *recordingPusher) got() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.texts...)
}

func countPaused(texts []string) int {
	n := 0
	for _, s := range texts {
		if strings.Contains(s, "DELIVERY PAUSED") {
			n++
		}
	}
	return n
}

func TestNoticesSpendTheSameWindowAsDeliveries(t *testing.T) {
	h := &Hub{}
	g := h.gate()
	for i := 0; i < 20; i++ {
		if !g.budget.Take("conv", fmt.Sprintf("c%d", i), 10) {
			t.Fatalf("delivery %d refused", i)
		}
	}
	p := &recordingPusher{}
	for i := 0; i < 10; i++ {
		g.push(p, fmt.Sprintf("notice %d", i))
	}
	got := p.got()
	if len(got) != 5 || countPaused(got) != 1 {
		t.Fatalf("with 20 deliveries out, notices pushed %v; want 4 notices and the pause", got)
	}
}

func TestWaitingNoticesFollowInOrderOnceAConfirmFreesRoom(t *testing.T) {
	h := &Hub{}
	g := h.gate()
	p := &recordingPusher{}
	for i := 0; i < 30; i++ {
		g.push(p, fmt.Sprintf("n%02d", i))
	}
	g.budget.ReleaseAll()
	var notices []string
	for _, s := range p.got() {
		if !strings.Contains(s, "PAUSED") {
			notices = append(notices, s)
		}
	}
	if len(notices) != 30 {
		t.Fatalf("%d notices delivered after the release, want 30", len(notices))
	}
	for i, s := range notices {
		if s != fmt.Sprintf("n%02d", i) {
			t.Fatalf("out of order at %d: %v", i, notices)
		}
	}
}

func TestANewNoticeNeverOvertakesOneThatIsWaiting(t *testing.T) {
	h := &Hub{}
	g := h.gate()
	p := &recordingPusher{}
	for i := 0; i < 26; i++ {
		g.push(p, fmt.Sprintf("n%d", i))
	}
	g.budget.Take("conv", "c", 1) // fails: the window is paused
	g.push(p, "newer")
	for _, s := range p.got() {
		if s == "newer" {
			t.Fatal("a new notice went out while an older one was still waiting")
		}
	}
}

func TestAConfirmWithoutArgumentsFreesTheWholeWindow(t *testing.T) {
	h := NewHub()
	for i := 0; i < 24; i++ {
		h.gate().budget.Take("conv", fmt.Sprintf("c%d", i), 10)
	}
	res, err := h.handleConfirmReceived(t.Context(), mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("confirm without arguments: %v %s", err, textOf(res))
	}
	if n, _ := h.gate().budget.Free(); n != 24 {
		t.Fatalf("free %d after the confirm, want 24", n)
	}
}
