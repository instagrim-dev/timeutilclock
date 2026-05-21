# timeutilclock

`timeutilclock` provides a lightweight clock abstraction for Go projects that need
deterministic control over time in tests without sacrificing production performance.

## Features

- Minimal `Clock` interface (`Now`, `Since`, `After`, `NewTicker`)
- `SystemClock` backed by `time` for production
- `MockClock` that can be advanced manually in tests
- Mock tickers emit each elapsed interval when time advances, with a bounded
  backlog and `RunDue` for flushing additional due ticks after callers drain
  ticker channels
- No external dependencies

## Usage

```go
import clock "github.com/instagrim-dev/timeutilclock"

func doWork(c clock.Clock) {
    start := c.Now()
    <-c.After(100 * time.Millisecond)
    fmt.Println("elapsed:", c.Since(start))
}

// Production
doWork(clock.NewSystemClock())

// Tests
mock := clock.NewMockClock(time.Now())
go func() {
    mock.Advance(100 * time.Millisecond)
}()
doWork(mock)
```

## Ticker backlog semantics

`MockClock.Advance` emits every ticker interval that elapsed, rather than
collapsing a large time jump into one tick. This makes cadence-sensitive tests
more useful for UI loops, animation loops, retry loops, and coalescing logic.

Ticker channels are still bounded. `NewMockClock` uses a sensible default
backlog; use `NewMockClockWithTickerBuffer` when a test needs a smaller or
larger buffer. If an advance produces more due ticks than the channel can hold,
drain the ticker channel and call `RunDue` to flush the remaining due ticks at
the current mock time.

This repository is split from `platform-libraries/pkg/timeutilclock` so the
clock utility can be versioned and consumed independently.
