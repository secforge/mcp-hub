package harness

import (
	"testing"
	"time"
)

func TestTheBurstPassesAndTheRestIsSpacedOut(t *testing.T) {
	p := newPacer(3, 50*time.Millisecond)
	start := time.Now()
	for i := 0; i < 3; i++ {
		p.wait()
	}
	if d := time.Since(start); d > 20*time.Millisecond {
		t.Fatalf("the burst waited %s", d)
	}
	p.wait()
	p.wait()
	if d := time.Since(start); d < 90*time.Millisecond {
		t.Fatalf("two pushes past the burst took only %s", d)
	}
}
