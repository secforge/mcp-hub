package harness

import (
	"sync"
	"time"
)

// pace keeps this process's pushes inside the receiving session's rate
// limit. Claude Code 2.1.x lets a sender burst 30 messages and then one
// every 2 s; beyond that it drops the message, saying so only in its own
// UI, so the push reports success and nothing here learns of the loss.
// The sender is keyed by its reply address or, without one, by process,
// so every connection of this process shares the one bucket. Seen live
// on 2026-10-20: the dropped message was the delivery-paused notice, and
// the session was never told its deliveries had stopped.
//
// Kept below the limit on both counts, and waited for rather than
// exceeded: a late message costs seconds, a dropped one is gone.
var pace = newPacer(20, 2500*time.Millisecond)

type pacer struct {
	mu       sync.Mutex
	capacity float64
	every    time.Duration
	tokens   float64
	last     time.Time
}

func newPacer(capacity int, every time.Duration) *pacer {
	return &pacer{capacity: float64(capacity), every: every, tokens: float64(capacity)}
}

// wait takes one token, sleeping until one is there.
func (p *pacer) wait() {
	for {
		p.mu.Lock()
		now := time.Now()
		if !p.last.IsZero() {
			p.tokens += float64(now.Sub(p.last)) / float64(p.every)
			if p.tokens > p.capacity {
				p.tokens = p.capacity
			}
		}
		p.last = now
		if p.tokens >= 1 {
			p.tokens--
			p.mu.Unlock()
			return
		}
		short := time.Duration((1 - p.tokens) * float64(p.every))
		p.mu.Unlock()
		time.Sleep(short)
	}
}
