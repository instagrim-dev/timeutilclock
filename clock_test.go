package timeutilclock

import (
	"testing"
	"time"
)

func TestSystemClock_Basic(t *testing.T) {
	clock := NewSystemClock()
	now := clock.Now()
	if clock.Since(now) < 0 {
		t.Fatal("system clock reported negative duration")
	}

	select {
	case <-clock.After(10 * time.Millisecond):
	case <-time.After(100 * time.Millisecond):
		t.Fatal("system clock After did not fire")
	}
}

func TestMockClockAdvance(t *testing.T) {
	start := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
	clock := NewMockClock(start)

	if got := clock.Now(); !got.Equal(start) {
		t.Fatalf("expected start time %v, got %v", start, got)
	}

	ch := clock.After(5 * time.Second)
	select {
	case <-ch:
		t.Fatal("After fired before advance")
	default:
	}

	clock.Advance(3 * time.Second)
	select {
	case <-ch:
		t.Fatal("After fired too early")
	default:
	}

	clock.Advance(2 * time.Second)
	select {
	case ts := <-ch:
		if !ts.Equal(clock.Now()) {
			t.Fatalf("expected After timestamp %v, got %v", clock.Now(), ts)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("After did not fire after advance")
	}
}

func TestMockClockTicker(t *testing.T) {
	start := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	clock := NewMockClock(start)
	ticker := clock.NewTicker(time.Second)
	defer ticker.Stop()

	clock.Advance(time.Second)
	select {
	case <-ticker.C():
	default:
		t.Fatal("expected ticker to fire after first advance")
	}

	clock.Advance(time.Second)
	select {
	case <-ticker.C():
	default:
		t.Fatal("expected ticker to fire after second advance")
	}
}

func TestMockClockTickerEmitsEachElapsedInterval(t *testing.T) {
	start := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	clock := NewMockClock(start)
	ticker := clock.NewTicker(time.Second)
	defer ticker.Stop()

	clock.Advance(5 * time.Second)

	for i := 1; i <= 5; i++ {
		want := start.Add(time.Duration(i) * time.Second)
		select {
		case got := <-ticker.C():
			if !got.Equal(want) {
				t.Fatalf("tick %d: expected %v, got %v", i, want, got)
			}
		default:
			t.Fatalf("expected tick %d after a five-second advance", i)
		}
	}
}

func TestMockClockRunDueFlushesBackloggedTicker(t *testing.T) {
	start := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	clock := NewMockClockWithTickerBuffer(start, 2)
	ticker := clock.NewTicker(time.Second)
	defer ticker.Stop()

	clock.Advance(5 * time.Second)
	requireTick(t, ticker, start.Add(time.Second))
	requireTick(t, ticker, start.Add(2*time.Second))

	clock.RunDue()
	requireTick(t, ticker, start.Add(3*time.Second))
	requireTick(t, ticker, start.Add(4*time.Second))

	clock.RunDue()
	requireTick(t, ticker, start.Add(5*time.Second))
}

func requireTick(t *testing.T, ticker Ticker, want time.Time) {
	t.Helper()
	select {
	case got := <-ticker.C():
		if !got.Equal(want) {
			t.Fatalf("expected tick %v, got %v", want, got)
		}
	default:
		t.Fatalf("expected tick %v", want)
	}
}
