package timeutilclock

import (
	"sync"
	"time"
)

// Clock defines the contract for interacting with time. It is deliberately minimal so that
// production code can rely on wall-clock behaviour while tests can inject deterministic clocks.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// Since returns the duration that has elapsed since t.
	Since(t time.Time) time.Duration
	// After returns a channel that will deliver the current time once the duration has elapsed.
	After(d time.Duration) <-chan time.Time
	// NewTicker creates a ticker that emits ticks on the returned channel.
	NewTicker(d time.Duration) Ticker
}

// Ticker mirrors time.Ticker but is abstracted for testability.
type Ticker interface {
	C() <-chan time.Time
	Stop()
	Reset(d time.Duration)
}

// SystemClock implements Clock using the standard library time package.
type SystemClock struct{}

// NewSystemClock constructs a Clock backed by the wall clock.
func NewSystemClock() *SystemClock {
	return &SystemClock{}
}

func (sc *SystemClock) Now() time.Time {
	return time.Now()
}

func (sc *SystemClock) Since(t time.Time) time.Duration {
	return time.Since(t)
}

func (sc *SystemClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (sc *SystemClock) NewTicker(d time.Duration) Ticker {
	return &systemTicker{ticker: time.NewTicker(d)}
}

type systemTicker struct {
	ticker *time.Ticker
}

func (st *systemTicker) C() <-chan time.Time {
	return st.ticker.C
}

func (st *systemTicker) Stop() {
	st.ticker.Stop()
}

func (st *systemTicker) Reset(d time.Duration) {
	st.ticker.Reset(d)
}

// MockClock provides a deterministic clock for tests. Time only
// advances when Advance or Set are invoked which keeps unit tests fast.
type MockClock struct {
	mu           sync.RWMutex
	currentTime  time.Time
	afterChans   []afterWaiter
	tickers      []*mockTicker
	tickerBuffer int
}

// afterWaiter tracks a pending After request.
type afterWaiter struct {
	target time.Time
	ch     chan time.Time
}

// NewMockClock initializes a mock clock anchored at the supplied time.
func NewMockClock(initial time.Time) *MockClock {
	return NewMockClockWithTickerBuffer(initial, defaultMockTickerBuffer)
}

const defaultMockTickerBuffer = 64

// NewMockClockWithTickerBuffer initializes a mock clock with a bounded ticker
// backlog. A larger buffer allows Advance to emit more elapsed intervals before
// callers need to drain ticker channels and call RunDue.
func NewMockClockWithTickerBuffer(initial time.Time, tickerBuffer int) *MockClock {
	if tickerBuffer < 1 {
		tickerBuffer = 1
	}
	return &MockClock{
		currentTime:  initial,
		afterChans:   make([]afterWaiter, 0),
		tickers:      make([]*mockTicker, 0),
		tickerBuffer: tickerBuffer,
	}
}

func (mc *MockClock) Now() time.Time {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.currentTime
}

func (mc *MockClock) Since(t time.Time) time.Duration {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.currentTime.Sub(t)
}

func (mc *MockClock) After(d time.Duration) <-chan time.Time {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	ch := make(chan time.Time, 1)
	waiter := afterWaiter{
		target: mc.currentTime.Add(d),
		ch:     ch,
	}
	mc.afterChans = append(mc.afterChans, waiter)
	mc.tryFireAfterLocked()
	return ch
}

func (mc *MockClock) NewTicker(d time.Duration) Ticker {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mt := &mockTicker{
		clock:    mc,
		interval: d,
		ch:       make(chan time.Time, mc.tickerBuffer),
		stopCh:   make(chan struct{}),
		lastTick: mc.currentTime,
	}

	mc.tickers = append(mc.tickers, mt)
	return mt
}

// Set moves the clock to an absolute time.
func (mc *MockClock) Set(t time.Time) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.currentTime = t
	mc.tryFireAfterLocked()
	mc.tickTickersLocked()
}

// Advance moves the clock forward by d.
func (mc *MockClock) Advance(d time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.currentTime = mc.currentTime.Add(d)
	mc.tryFireAfterLocked()
	mc.tickTickersLocked()
}

// RunDue fires any timers or ticker intervals due at the current mock time.
// This is useful after draining bounded ticker channels when a large Advance
// produced more due ticks than the configured ticker backlog can hold.
func (mc *MockClock) RunDue() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.tryFireAfterLocked()
	mc.tickTickersLocked()
}

func (mc *MockClock) tryFireAfterLocked() {
	if len(mc.afterChans) == 0 {
		return
	}

	pending := mc.afterChans[:0]
	for _, waiter := range mc.afterChans {
		if !mc.currentTime.Before(waiter.target) {
			select {
			case waiter.ch <- mc.currentTime:
			default:
			}
		} else {
			pending = append(pending, waiter)
		}
	}
	mc.afterChans = pending
}

func (mc *MockClock) tickTickersLocked() {
	for _, ticker := range mc.tickers {
		ticker.tickLocked(mc.currentTime)
	}
}

type mockTicker struct {
	clock    *MockClock
	interval time.Duration
	ch       chan time.Time
	stopCh   chan struct{}

	mu       sync.RWMutex
	lastTick time.Time
	stopped  bool
}

func (mt *mockTicker) C() <-chan time.Time {
	return mt.ch
}

func (mt *mockTicker) Stop() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if !mt.stopped {
		mt.stopped = true
		close(mt.stopCh)
	}
}

func (mt *mockTicker) Reset(d time.Duration) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.interval = d
	mt.lastTick = mt.clock.Now()
}

func (mt *mockTicker) tickLocked(now time.Time) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.stopped || mt.interval <= 0 {
		return
	}

	for !mt.lastTick.Add(mt.interval).After(now) {
		next := mt.lastTick.Add(mt.interval)
		select {
		case mt.ch <- next:
			mt.lastTick = next
		default:
			return
		}
	}
}
