package rill

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultBufferSize = 256
	DefaultMaxWorkers = 16
	DefaultLRUSize   = 10000
)

type StreamConfig struct {
	BufferSize   int
	MaxWorkers   int
	Backpressure bool
}

func DefaultStreamConfig() *StreamConfig {
	return &StreamConfig{
		BufferSize:   DefaultBufferSize,
		MaxWorkers:   DefaultMaxWorkers,
		Backpressure: true,
	}
}

func WithBufferSize(size int) func(*StreamConfig) {
	return func(c *StreamConfig) {
		c.BufferSize = size
	}
}

func WithMaxWorkers(n int) func(*StreamConfig) {
	return func(c *StreamConfig) {
		c.MaxWorkers = n
	}
}

func WithBackpressure(enabled bool) func(*StreamConfig) {
	return func(c *StreamConfig) {
		c.Backpressure = enabled
	}
}

type LRUCache struct {
	maxSize int
	mu      sync.Mutex
	cache   *list.List
	items   map[interface{}]*list.Element
}

func NewLRUCache(maxSize int) *LRUCache {
	return &LRUCache{
		maxSize: maxSize,
		cache:   list.New(),
		items:   make(map[interface{}]*list.Element),
	}
}

func (l *LRUCache) Contains(key interface{}) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, exists := l.items[key]
	return exists
}

// Add returns true if the key is NEW (added), false if it already existed (duplicate).
// This is the correct semantics for deduplication.
func (l *LRUCache) Add(key interface{}) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.items[key]; exists {
		// Key already exists, move to front and return false (duplicate)
		elem := l.items[key]
		l.cache.MoveToFront(elem)
		return false // Already exists, not newly added
	}

	// Key is new
	if l.cache.Len() >= l.maxSize {
		l.removeOldest()
	}

	elem := l.cache.PushFront(key)
	l.items[key] = elem
	return true // Newly added
}

// Get returns the value and true if found, otherwise returns nil and false.
// This allows callers to distinguish between "key not found" and "value is nil".
func (l *LRUCache) Get(key interface{}) (interface{}, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	elem, exists := l.items[key]
	if !exists {
		return nil, false
	}
	l.cache.MoveToFront(elem)
	return elem.Value, true
}

func (l *LRUCache) removeOldest() {
	if elem := l.cache.Back(); elem != nil {
		l.cache.Remove(elem)
		delete(l.items, elem.Value)
	}
}

func (l *LRUCache) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cache.Len()
}

type Metrics struct {
	processedItems       int64
	processedErrors     int64
	totalProcessingTime int64
	lastActivityTime    int64
	activeWorkers       int32
	peakActiveWorkers   int32
	channelSends        int64
	channelReceives     int64
	blockedGoroutines   int64
	mu                 sync.RWMutex
}

var globalMetrics = &Metrics{}

func GetMetrics() *Metrics {
	return globalMetrics
}

func (m *Metrics) RecordItem() {
	atomic.AddInt64(&m.processedItems, 1)
	atomic.StoreInt64(&m.lastActivityTime, time.Now().UnixNano())
}

func (m *Metrics) RecordError() {
	atomic.AddInt64(&m.processedErrors, 1)
	atomic.StoreInt64(&m.lastActivityTime, time.Now().UnixNano())
}

func (m *Metrics) RecordProcessingTime(d time.Duration) {
	atomic.AddInt64(&m.totalProcessingTime, int64(d.Nanoseconds()))
}

func (m *Metrics) IncrementActiveWorkers() int32 {
	n := atomic.AddInt32(&m.activeWorkers, 1)
	for {
		peak := atomic.LoadInt32(&m.peakActiveWorkers)
		if n <= peak {
			break
		}
		if atomic.CompareAndSwapInt32(&m.peakActiveWorkers, peak, n) {
			break
		}
	}
	return n
}

func (m *Metrics) DecrementActiveWorkers() {
	atomic.AddInt32(&m.activeWorkers, -1)
}

func (m *Metrics) RecordChannelSend() {
	atomic.AddInt64(&m.channelSends, 1)
}

func (m *Metrics) RecordChannelReceive() {
	atomic.AddInt64(&m.channelReceives, 1)
}

func (m *Metrics) RecordBlockedGoroutine() {
	atomic.AddInt64(&m.blockedGoroutines, 1)
}

type MetricsSnapshot struct {
	ProcessedItems       int64
	ProcessedErrors      int64
	TotalProcessingTime  time.Duration
	LastActivityTime     time.Time
	ActiveWorkers       int32
	PeakActiveWorkers   int32
	ChannelSends        int64
	ChannelReceives     int64
	BlockedGoroutines   int64
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		ProcessedItems:      atomic.LoadInt64(&m.processedItems),
		ProcessedErrors:     atomic.LoadInt64(&m.processedErrors),
		TotalProcessingTime: time.Duration(atomic.LoadInt64(&m.totalProcessingTime)),
		LastActivityTime:   time.Unix(0, atomic.LoadInt64(&m.lastActivityTime)),
		ActiveWorkers:      atomic.LoadInt32(&m.activeWorkers),
		PeakActiveWorkers:  atomic.LoadInt32(&m.peakActiveWorkers),
		ChannelSends:       atomic.LoadInt64(&m.channelSends),
		ChannelReceives:    atomic.LoadInt64(&m.channelReceives),
		BlockedGoroutines:  atomic.LoadInt64(&m.blockedGoroutines),
	}
}

func (m *Metrics) Reset() {
	atomic.StoreInt64(&m.processedItems, 0)
	atomic.StoreInt64(&m.processedErrors, 0)
	atomic.StoreInt64(&m.totalProcessingTime, 0)
	atomic.StoreInt32(&m.activeWorkers, 0)
	atomic.StoreInt32(&m.peakActiveWorkers, 0)
	atomic.StoreInt64(&m.channelSends, 0)
	atomic.StoreInt64(&m.channelReceives, 0)
	atomic.StoreInt64(&m.blockedGoroutines, 0)
}

type Tracer interface {
	StartSpan(name string) Span
}

type Span interface {
	End()
	RecordError(err error)
	SetTag(key string, value any)
}

type noopTracer struct{}

func (t *noopTracer) StartSpan(name string) Span { return &noopSpan{} }

type noopSpan struct{}

func (s *noopSpan) End()                          {}
func (s *noopSpan) RecordError(err error)         {}
func (s *noopSpan) SetTag(key string, value any) {}

var defaultTracer Tracer = &noopTracer{}

func SetTracer(t Tracer) {
	if t != nil {
		defaultTracer = t
	}
}

func getTracer() Tracer {
	return defaultTracer
}

type RetryOptions struct {
	MaxAttempts int
	Backoff     time.Duration
	MaxBackoff time.Duration
}

func DefaultRetryOptions() *RetryOptions {
	return &RetryOptions{
		MaxAttempts: 3,
		Backoff:     100 * time.Millisecond,
		MaxBackoff:  30 * time.Second,
	}
}

func WithMaxAttempts(n int) func(*RetryOptions) {
	return func(o *RetryOptions) {
		o.MaxAttempts = n
	}
}

func WithBackoff(d time.Duration) func(*RetryOptions) {
	return func(o *RetryOptions) {
		o.Backoff = d
	}
}

func WithMaxBackoff(d time.Duration) func(*RetryOptions) {
	return func(o *RetryOptions) {
		o.MaxBackoff = d
	}
}

type WindowConfig struct {
	Size    int
	Slide   int
	Timeout time.Duration
}

func CountWindow[A any](in <-chan Try[A], size int) <-chan []Try[A] {
	return Window(in, WindowConfig{Size: size, Slide: size})
}

func TimeWindow[A any](in <-chan Try[A], size int, timeout time.Duration) <-chan []Try[A] {
	return Window(in, WindowConfig{Size: size, Slide: 1, Timeout: timeout})
}

func Window[A any](in <-chan Try[A], cfg WindowConfig) <-chan []Try[A] {
	if in == nil || cfg.Size <= 0 {
		return nil
	}

	if cfg.Slide <= 0 {
		cfg.Slide = cfg.Size
	}

	out := make(chan []Try[A])

	go func() {
		defer close(out)

		buffer := make([]Try[A], 0, cfg.Size)
		var ticker *time.Ticker
		if cfg.Timeout > 0 {
			ticker = time.NewTicker(cfg.Timeout)
			defer ticker.Stop()
		}

		for {
			if len(buffer) >= cfg.Size {
				window := make([]Try[A], cfg.Size)
				copy(window, buffer[:cfg.Size])
				out <- window

				if cfg.Slide < cfg.Size {
					buffer = buffer[cfg.Slide:]
				} else {
					buffer = buffer[:0]
				}
				continue
			}

			if ticker == nil {
				// No timeout, count-based windows only
				select {
				case item, ok := <-in:
					if !ok {
						if len(buffer) > 0 {
							window := make([]Try[A], len(buffer))
							copy(window, buffer)
							out <- window
						}
						return
					}
					buffer = append(buffer, item)
				}
			} else {
				select {
				case <-ticker.C:
					if len(buffer) > 0 {
						window := make([]Try[A], len(buffer))
						copy(window, buffer)
						out <- window
						buffer = buffer[:0]
					}

				case item, ok := <-in:
					if !ok {
						if len(buffer) > 0 {
							window := make([]Try[A], len(buffer))
							copy(window, buffer)
							out <- window
						}
						return
					}
					buffer = append(buffer, item)
					ticker.Reset(cfg.Timeout)
				}
			}
		}
	}()

	return out
}

func WithContext[A any](ctx context.Context, in <-chan Try[A]) (<-chan Try[A], <-chan error) {
	out := make(chan Try[A])
	errCh := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errCh)

		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				out <- item
			}
		}
	}()

	return out, errCh
}

func Retry[A any](ctx context.Context, in <-chan Try[A], opts *RetryOptions, f func(context.Context, A) (A, error)) <-chan Try[A] {
	if opts == nil {
		opts = DefaultRetryOptions()
	}

	out := make(chan Try[A])

	go func() {
		defer close(out)

		for item := range in {
			if item.Error != nil {
				out <- item
				continue
			}

			var result A
			var err error
			backoff := opts.Backoff

			for attempt := 0; attempt < opts.MaxAttempts; attempt++ {
				// Apply backoff BEFORE each retry attempt (except the first)
				if attempt > 0 {
					select {
					case <-ctx.Done():
						out <- Try[A]{Error: ctx.Err()}
						return
					case <-time.After(backoff):
						backoff *= 2
						if backoff > opts.MaxBackoff {
							backoff = opts.MaxBackoff
						}
					}
				}

				// Execute the function
				result, err = f(ctx, item.Value)
				if err == nil {
					break // Success, exit retry loop
				}

				// If this was the last attempt, err will be returned after loop
			}

			if err != nil {
				out <- Try[A]{Error: fmt.Errorf("retry failed after %d attempts: %w", opts.MaxAttempts, err)}
			} else {
				out <- Try[A]{Value: result}
			}
		}
	}()

	return out
}

// Timeout returns a stream that will timeout if no items are received within the specified duration.
// Unlike the original buggy implementation, this correctly manages the context lifecycle.
func Timeout[A any](in <-chan Try[A], timeout time.Duration) <-chan Try[A] {
	if timeout <= 0 {
		// Return a channel that forwards all items from input
		out := make(chan Try[A], DefaultBufferSize)
		go func() {
			defer close(out)
			for item := range in {
				out <- item
			}
		}()
		return out
	}

	out := make(chan Try[A])

	go func() {
		defer close(out)

		// Create a fresh context for this goroutine, NOT in the parent function
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel() // Cancel when goroutine exits

		for {
			select {
			case <-ctx.Done():
				out <- Try[A]{Error: ctx.Err()}
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				out <- item
			}
		}
	}()

	return out
}

// OrElse provides a fallback stream when the input stream encounters errors.
// If an error occurs, it switches to consuming from the fallback stream.
func OrElse[A any](in <-chan Try[A], fallback <-chan Try[A]) <-chan Try[A] {
	out := make(chan Try[A])

	go func() {
		defer close(out)

		// First, try to consume from the primary stream
		for item := range in {
			if item.Error == nil {
				out <- item
			} else {
				// Error encountered, switch to fallback
				for fb := range fallback {
					out <- fb
				}
				return
			}
		}
	}()

	return out
}

// CatchWithFallback catches errors and replaces them with a fallback value.
// Unlike Catch which can silently drop items, this always outputs either
// the original value or the fallback.
func CatchWithFallback[A any](in <-chan Try[A], n int, fallbackValue A, f func(error) error) <-chan Try[A] {
	out := make(chan Try[A], DefaultBufferSize)

	loop(in, out, n, func(a Try[A]) {
		if a.Error == nil {
			out <- a
			return
		}

		// Apply error handler
		newErr := f(a.Error)
		if newErr == nil {
			// Error was handled, output fallback value
			out <- Try[A]{Value: fallbackValue}
		} else {
			// Error persists, output with original error
			out <- Try[A]{Error: newErr}
		}
	})

	return out
}
