package mcptools

import (
	"sync"

	"github.com/secforge/harness-transport/deliver"

	"github.com/secforge/mcp-hub/internal/hubconn"
)

// pushGate delivers this client's own notices — drops, reconnects,
// conversations shared and withdrawn, catch-up summaries — through the
// process's one delivery window and its queue (hubconn.Budget.Deliver),
// the same window and queue every conversation's pushes use: a notice that
// does not fit waits behind whatever already waits, in arrival order.
type pushGate struct {
	budget *hubconn.Budget
}

// gatePusher is what the gate pushes through: a harness.Pusher, or a
// test's stand-in.
type gatePusher interface {
	Push(cursor, body string, more bool) (deliver.Receipt, error)
}

// gate returns this process's notice gate, on its delivery window.
func (h *Hub) gate() *pushGate {
	h.gateOnce.Do(func() {
		if h.budget == nil {
			h.budget = hubconn.NewDeliveryBudget()
		}
		h.pushGate = &pushGate{budget: h.budget}
	})
	return h.pushGate
}

// push delivers one of this client's own notices through p, now or in its
// turn. held reports the second; err only a push attempted now that
// failed.
func (g *pushGate) push(p gatePusher, text string) (held bool, err error) {
	var mu sync.Mutex
	held = g.budget.Deliver(hubconn.Delivery{
		Bytes: len(text),
		Send: func() {
			_, e := p.Push("", text, false)
			mu.Lock()
			err = e
			mu.Unlock()
		},
		Pause: func(paused string) { _, _ = p.Push("", paused, false) },
	})
	if held {
		return true, nil
	}
	mu.Lock()
	defer mu.Unlock()
	return false, err
}
