package rill

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

func Map[A, B any](in <-chan Try[A], n int, f func(A) (B, error)) <-chan Try[B] {
	out := make(chan Try[B], DefaultBufferSize)

	if n == 1 {
		go func() {
			defer close(out)
			for a := range in {
				globalMetrics.RecordChannelReceive()
				if a.Error != nil {
					globalMetrics.RecordError()
					out <- Try[B]{Error: a.Error}
					globalMetrics.RecordChannelSend()
					continue
				}
				start := time.Now()
				b, err := f(a.Value)
				duration := time.Since(start)
				globalMetrics.RecordProcessingTime(duration)
				if err != nil {
					globalMetrics.RecordError()
					out <- Try[B]{Error: err}
					globalMetrics.RecordChannelSend()
				} else {
					globalMetrics.RecordItem()
					out <- Try[B]{Value: b}
					globalMetrics.RecordChannelSend()
				}
			}
		}()
		return out
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		globalMetrics.IncrementActiveWorkers()
		go func() {
			defer wg.Done()
			defer globalMetrics.DecrementActiveWorkers()
			for a := range in {
				globalMetrics.RecordChannelReceive()
				if a.Error != nil {
					globalMetrics.RecordError()
					out <- Try[B]{Error: a.Error}
					globalMetrics.RecordChannelSend()
					continue
				}
				start := time.Now()
				b, err := f(a.Value)
				duration := time.Since(start)
				globalMetrics.RecordProcessingTime(duration)
				if err != nil {
					globalMetrics.RecordError()
					out <- Try[B]{Error: err}
					globalMetrics.RecordChannelSend()
				} else {
					globalMetrics.RecordItem()
					out <- Try[B]{Value: b}
					globalMetrics.RecordChannelSend()
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// loop is a simple parallel worker pool that distributes work across n goroutines.
func loop[A, B any](in <-chan A, out chan<- B, n int, f func(A)) {
	if in == nil {
		close(out)
		return
	}

	// Always run in a goroutine to prevent deadlocks
	go func() {
		if n <= 1 {
			for a := range in {
				f(a)
			}
			close(out)
			return
		}

		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for a := range in {
					f(a)
				}
			}()
		}
		wg.Wait()
		close(out)
	}()
}

// loopWithContext is a parallel worker pool that respects context cancellation.
func loopWithContext[A, B any](ctx context.Context, in <-chan A, done chan<- B, n int, f func(context.Context, A)) {
	if ctx == nil {
		ctx = context.Background()
	}
	if in == nil {
		return
	}

	if n <= 1 {
		for {
			select {
			case <-ctx.Done():
				return
			case a, ok := <-in:
				if !ok {
					return
				}
				f(ctx, a)
			}
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					f(ctx, a)
				}
			}
		}()
	}
	wg.Wait()
	if done != nil {
		close(done)
	}
}

func MapWithContext[A, B any](ctx context.Context, in <-chan Try[A], n int, f func(context.Context, A) (B, error)) <-chan Try[B] {
	if ctx == nil {
		ctx = context.Background()
	}

	out := make(chan Try[B], DefaultBufferSize)

	if n == 1 {
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					if a.Error != nil {
						out <- Try[B]{Error: a.Error}
						continue
					}
					b, err := f(ctx, a.Value)
					if err != nil {
						out <- Try[B]{Error: err}
					} else {
						out <- Try[B]{Value: b}
					}
				}
			}
		}()
		return out
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					if a.Error != nil {
						out <- Try[B]{Error: a.Error}
						continue
					}
					b, err := f(ctx, a.Value)
					if err != nil {
						out <- Try[B]{Error: err}
					} else {
						out <- Try[B]{Value: b}
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func OrderedMap[A, B any](in <-chan Try[A], n int, f func(A) (B, error)) <-chan Try[B] {
	return OrderedMapWithContext(context.Background(), in, n, func(ctx context.Context, a A) (B, error) {
		return f(a)
	})
}

func OrderedMapWithContext[A, B any](ctx context.Context, in <-chan Try[A], n int, f func(context.Context, A) (B, error)) <-chan Try[B] {
	if ctx == nil {
		ctx = context.Background()
	}

	out := make(chan Try[B], DefaultBufferSize)
	orderedLoopWithContext(ctx, in, out, n, func(ctx context.Context, a Try[A], canWrite <-chan struct{}) {
		var result Try[B]
		if a.Error != nil {
			result = Try[B]{Error: a.Error}
		} else {
			b, err := f(ctx, a.Value)
			if err != nil {
				result = Try[B]{Error: err}
			} else {
				result = Try[B]{Value: b}
			}
		}
		if canWrite != nil {
			<-canWrite
		}
		out <- result
	})
	return out
}

func OrderedFlatMap[A, B any](in <-chan Try[A], n int, f func(A) <-chan Try[B]) <-chan Try[B] {
	return OrderedFlatMapWithContext(context.Background(), in, n, func(ctx context.Context, a A) <-chan Try[B] {
		return f(a)
	})
}

func OrderedFlatMapWithContext[A, B any](ctx context.Context, in <-chan Try[A], n int, f func(context.Context, A) <-chan Try[B]) <-chan Try[B] {
	if ctx == nil {
		ctx = context.Background()
	}

	out := make(chan Try[B], DefaultBufferSize)

	// Use a map to collect outputs for each sequence number, and a mutex to protect it
	type result struct {
		items []Try[B]
		done  bool
	}
	var (
		mu       sync.Mutex
		cond     = sync.NewCond(&mu)
		results  = make(map[int]result)
		nextOut  = 0
		workersDone = false
	)

	// Sequence counter
	var seq int32 = 0

	// Worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					mySeq := int(atomic.AddInt32(&seq, 1)) - 1

					if a.Error != nil {
						mu.Lock()
						results[mySeq] = result{
							items: []Try[B]{{Error: a.Error}},
							done:  true,
						}
						cond.Signal()
						mu.Unlock()
					} else {
						// Collect all items from the flatMap
						var items []Try[B]
						for b := range f(ctx, a.Value) {
							items = append(items, b)
						}
						mu.Lock()
						results[mySeq] = result{
							items: items,
							done:  true,
						}
						cond.Signal()
						mu.Unlock()
					}
				}
			}
		}()
	}

	// Output goroutine: preserves order using sync.Cond
	go func() {
		defer close(out)
		for {
			mu.Lock()
			// Check if we have the next sequence to output
			if r, ok := results[nextOut]; ok && r.done {
				// Output all items for this sequence
				for _, item := range r.items {
					out <- item
				}
				delete(results, nextOut)
				nextOut++
				mu.Unlock()
				// After successful output, check if we should continue or wait
				continue
			} else if workersDone && len(results) == 0 {
				// All workers done and no more results to output
				mu.Unlock()
				return
			} else {
				// Wait for new results
				cond.Wait()
				mu.Unlock()
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	// Wait for workers to finish
	go func() {
		wg.Wait()
		mu.Lock()
		workersDone = true
		mu.Unlock()
		cond.Signal() // Wake up output goroutine to check if there are remaining results
	}()

	return out
}

func Filter[A any](in <-chan Try[A], n int, f func(A) (bool, error)) <-chan Try[A] {
	out := make(chan Try[A], DefaultBufferSize)

	if n == 1 {
		go func() {
			defer close(out)
			for a := range in {
				globalMetrics.RecordChannelReceive()
				if a.Error != nil {
					globalMetrics.RecordError()
					out <- a
					globalMetrics.RecordChannelSend()
					continue
				}
				keep, err := f(a.Value)
				if err != nil {
					globalMetrics.RecordError()
					out <- Try[A]{Error: err}
					globalMetrics.RecordChannelSend()
				} else if keep {
					globalMetrics.RecordItem()
					out <- a
					globalMetrics.RecordChannelSend()
				}
			}
		}()
		return out
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		globalMetrics.IncrementActiveWorkers()
		go func() {
			defer wg.Done()
			defer globalMetrics.DecrementActiveWorkers()
			for a := range in {
				globalMetrics.RecordChannelReceive()
				if a.Error != nil {
					globalMetrics.RecordError()
					out <- a
					globalMetrics.RecordChannelSend()
					continue
				}
				keep, err := f(a.Value)
				if err != nil {
					globalMetrics.RecordError()
					out <- Try[A]{Error: err}
					globalMetrics.RecordChannelSend()
				} else if keep {
					globalMetrics.RecordItem()
					out <- a
					globalMetrics.RecordChannelSend()
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func FilterWithContext[A any](ctx context.Context, in <-chan Try[A], n int, f func(context.Context, A) (bool, error)) <-chan Try[A] {
	if ctx == nil {
		ctx = context.Background()
	}

	out := make(chan Try[A], DefaultBufferSize)

	if n == 1 {
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					if a.Error != nil {
						out <- a
						continue
					}
					keep, err := f(ctx, a.Value)
					if err != nil {
						out <- Try[A]{Error: err}
					} else if keep {
						out <- a
					}
				}
			}
		}()
		return out
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					if a.Error != nil {
						out <- a
						continue
					}
					keep, err := f(ctx, a.Value)
					if err != nil {
						out <- Try[A]{Error: err}
					} else if keep {
						out <- a
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func OrderedFilter[A any](in <-chan Try[A], n int, f func(A) (bool, error)) <-chan Try[A] {
	return OrderedFilterWithContext(context.Background(), in, n, func(ctx context.Context, a A) (bool, error) {
		return f(a)
	})
}

func OrderedFilterWithContext[A any](ctx context.Context, in <-chan Try[A], n int, f func(context.Context, A) (bool, error)) <-chan Try[A] {
	if ctx == nil {
		ctx = context.Background()
	}

	out := make(chan Try[A], DefaultBufferSize)
	orderedLoopWithContext(ctx, in, out, n, func(ctx context.Context, a Try[A], canWrite <-chan struct{}) {
		if a.Error != nil {
			if canWrite != nil {
				<-canWrite
			}
			out <- a
			return
		}
		keep, err := f(ctx, a.Value)
		if err != nil {
			if canWrite != nil {
				<-canWrite
			}
			out <- Try[A]{Error: err}
			return
		}
		if canWrite != nil {
			<-canWrite
		}
		if keep {
			out <- a
		}
	})
	return out
}

func FilterMap[A, B any](in <-chan Try[A], n int, f func(A) (B, bool, error)) <-chan Try[B] {
	return FilterMapWithContext(context.Background(), in, n, func(ctx context.Context, a A) (B, bool, error) {
		return f(a)
	})
}

func FilterMapWithContext[A, B any](ctx context.Context, in <-chan Try[A], n int, f func(context.Context, A) (B, bool, error)) <-chan Try[B] {
	if ctx == nil {
		ctx = context.Background()
	}

	out := make(chan Try[B], DefaultBufferSize)

	if n == 1 {
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					globalMetrics.RecordChannelReceive()
					if a.Error != nil {
						globalMetrics.RecordError()
						out <- Try[B]{Error: a.Error}
						globalMetrics.RecordChannelSend()
						continue
					}
					start := time.Now()
					b, keep, err := f(ctx, a.Value)
					duration := time.Since(start)
					globalMetrics.RecordProcessingTime(duration)
					if err != nil {
						globalMetrics.RecordError()
						out <- Try[B]{Error: err}
						globalMetrics.RecordChannelSend()
					} else if keep {
						globalMetrics.RecordItem()
						out <- Try[B]{Value: b}
						globalMetrics.RecordChannelSend()
					} else {
						globalMetrics.RecordChannelSend()
					}
				}
			}
		}()
		return out
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		globalMetrics.IncrementActiveWorkers()
		go func() {
			defer wg.Done()
			defer globalMetrics.DecrementActiveWorkers()
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					globalMetrics.RecordChannelReceive()
					if a.Error != nil {
						globalMetrics.RecordError()
						out <- Try[B]{Error: a.Error}
						globalMetrics.RecordChannelSend()
						continue
					}
					start := time.Now()
					b, keep, err := f(ctx, a.Value)
					duration := time.Since(start)
					globalMetrics.RecordProcessingTime(duration)
					if err != nil {
						globalMetrics.RecordError()
						out <- Try[B]{Error: err}
						globalMetrics.RecordChannelSend()
					} else if keep {
						globalMetrics.RecordItem()
						out <- Try[B]{Value: b}
						globalMetrics.RecordChannelSend()
					} else {
						globalMetrics.RecordChannelSend()
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func FlatMap[A, B any](in <-chan Try[A], n int, f func(A) <-chan Try[B]) <-chan Try[B] {
	return FlatMapWithContext(context.Background(), in, n, func(ctx context.Context, a A) <-chan Try[B] {
		return f(a)
	})
}

func FlatMapWithContext[A, B any](ctx context.Context, in <-chan Try[A], n int, f func(context.Context, A) <-chan Try[B]) <-chan Try[B] {
	if ctx == nil {
		ctx = context.Background()
	}

	out := make(chan Try[B], DefaultBufferSize)

	if n == 1 {
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					globalMetrics.RecordChannelReceive()
					if a.Error != nil {
						globalMetrics.RecordError()
						out <- Try[B]{Error: a.Error}
						globalMetrics.RecordChannelSend()
						continue
					}
					bb := f(ctx, a.Value)
					for b := range bb {
						globalMetrics.RecordItem()
						out <- b
						globalMetrics.RecordChannelSend()
					}
				}
			}
		}()
		return out
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		globalMetrics.IncrementActiveWorkers()
		go func() {
			defer wg.Done()
			defer globalMetrics.DecrementActiveWorkers()
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					globalMetrics.RecordChannelReceive()
					if a.Error != nil {
						globalMetrics.RecordError()
						out <- Try[B]{Error: a.Error}
						globalMetrics.RecordChannelSend()
						continue
					}
					bb := f(ctx, a.Value)
					for b := range bb {
						globalMetrics.RecordItem()
						out <- b
						globalMetrics.RecordChannelSend()
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func Catch[A any](in <-chan Try[A], n int, f func(error) error) <-chan Try[A] {
	return CatchWithContext(context.Background(), in, n, func(ctx context.Context, err error) error {
		return f(err)
	})
}

func CatchWithContext[A any](ctx context.Context, in <-chan Try[A], n int, f func(context.Context, error) error) <-chan Try[A] {
	if ctx == nil {
		ctx = context.Background()
	}

	out := make(chan Try[A], DefaultBufferSize)

	if n == 1 {
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					globalMetrics.RecordChannelReceive()
					if a.Error == nil {
						globalMetrics.RecordItem()
						out <- a
						globalMetrics.RecordChannelSend()
						continue
					}
					err := f(ctx, a.Error)
					if err != nil {
						out <- Try[A]{Error: err}
						globalMetrics.RecordChannelSend()
					} else {
						// Error was handled/cancelled, still record as processed
						globalMetrics.RecordItem()
						globalMetrics.RecordChannelSend()
					}
				}
			}
		}()
		return out
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		globalMetrics.IncrementActiveWorkers()
		go func() {
			defer wg.Done()
			defer globalMetrics.DecrementActiveWorkers()
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					globalMetrics.RecordChannelReceive()
					if a.Error == nil {
						globalMetrics.RecordItem()
						out <- a
						globalMetrics.RecordChannelSend()
						continue
					}
					err := f(ctx, a.Error)
					if err != nil {
						out <- Try[A]{Error: err}
						globalMetrics.RecordChannelSend()
					} else {
						// Error was handled/cancelled, still record as processed
						globalMetrics.RecordItem()
						globalMetrics.RecordChannelSend()
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

type orderedLoopState struct {
	mu      sync.Mutex
	closed  bool
	signals map[int]chan struct{}
}

func newOrderedLoopState() *orderedLoopState {
	return &orderedLoopState{
		signals: make(map[int]chan struct{}),
	}
}

func (s *orderedLoopState) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for _, ch := range s.signals {
		close(ch)
	}
}

func orderedLoopWithContext[A, B any](ctx context.Context, in <-chan A, done chan<- B, n int, f func(context.Context, A, <-chan struct{})) {
	if ctx == nil {
		ctx = context.Background()
	}

	state := newOrderedLoopState()
	// Don't defer state.close() here - it would close signal channels while workers are still running
	// The state will be garbage collected when no longer referenced

	if n == 1 {
		semaphore := make(chan struct{}, 1)
		semaphore <- struct{}{}

		go func() {
			defer func() {
				if done != nil {
					close(done)
				}
			}()
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-in:
					if !ok {
						return
					}
					f(ctx, a, semaphore)
				}
			}
		}()
		return
	}

	// Use a global sequence tracker protected by mutex to avoid data races
	var nextSeq int32 = 0
	orderedIn := make(chan orderedValue[A], n*2)
	semaphore := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		semaphore <- struct{}{}
	}

	go func() {
		defer close(orderedIn)

		seq := 0
		for a := range in {
			select {
			case <-ctx.Done():
				return
			case <-semaphore:
				orderedIn <- orderedValue[A]{Value: a, Sequence: seq}
				semaphore <- struct{}{}
				seq++
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ov := range orderedIn {
				select {
				case <-ctx.Done():
					return
				default:
					// Atomically check if this is the next sequence we should process
					currentSeq := atomic.LoadInt32(&nextSeq)
					if ov.Sequence == int(currentSeq) {
						// This is the next expected sequence, process immediately
						atomic.AddInt32(&nextSeq, 1)
						f(ctx, ov.Value, nil)

						// After processing, try to wake up the next waiting goroutine
						for {
							expectedSeq := int(atomic.LoadInt32(&nextSeq))
							state.mu.Lock()
							if ch, ok := state.signals[expectedSeq]; ok {
								delete(state.signals, expectedSeq)
								state.mu.Unlock()
								// Signal the waiting goroutine
								select {
								case ch <- struct{}{}:
								default:
								}
								atomic.StoreInt32(&nextSeq, int32(expectedSeq+1))
							} else {
								state.mu.Unlock()
								break
							}
						}
					} else {
						// Out of order, need to wait for the correct sequence
						state.mu.Lock()
						state.signals[ov.Sequence] = make(chan struct{}, 1)
						ch := state.signals[ov.Sequence]
						state.mu.Unlock()

						// Wait for our turn with context cancellation check
						select {
						case <-ch:
						case <-ctx.Done():
							return
						}

						// Clean up
						state.mu.Lock()
						delete(state.signals, ov.Sequence)
						state.mu.Unlock()

						// Update nextSeq and process - don't chain, let in-order branch handle subsequent wakes
						atomic.AddInt32(&nextSeq, 1)
						f(ctx, ov.Value, nil)
					}
				}
			}
		}()
	}

	if done != nil {
		go func() {
			wg.Wait()
			close(done)
		}()
	}
}

type orderedValue[A any] struct {
	Value    A
	Sequence int
}

func FanOut[A any](ctx context.Context, in <-chan Try[A], n int) []<-chan Try[A] {
	if ctx == nil {
		ctx = context.Background()
	}

	if in == nil || n <= 0 {
		return nil
	}

	outputs := make([]chan Try[A], n)
	for i := 0; i < n; i++ {
		outputs[i] = make(chan Try[A], DefaultBufferSize)
	}

	go func() {
		defer func() {
			for _, out := range outputs {
				close(out)
			}
		}()

		i := 0
		for item := range in {
			select {
			case <-ctx.Done():
				return
			case outputs[i] <- item:
			}
			i = (i + 1) % n
		}
	}()

	result := make([]<-chan Try[A], n)
	for i, ch := range outputs {
		result[i] = ch
	}
	return result
}

func FanIn[A any](ctx context.Context, ins ...<-chan Try[A]) <-chan Try[A] {
	if ctx == nil {
		ctx = context.Background()
	}

	if len(ins) == 0 {
		return nil
	}

	if len(ins) == 1 {
		return ins[0]
	}

	out := make(chan Try[A], DefaultBufferSize)
	var wg sync.WaitGroup

	for _, in := range ins {
		wg.Add(1)
		go func(ch <-chan Try[A]) {
			defer wg.Done()
			for item := range ch {
				select {
				case <-ctx.Done():
					return
				case out <- item:
				}
			}
		}(in)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func Throttle[A any](ctx context.Context, in <-chan Try[A], rate int, burst int) <-chan Try[A] {
	if ctx == nil {
		ctx = context.Background()
	}

	if rate <= 0 {
		return in
	}

	if burst <= 0 {
		burst = rate
	}

	out := make(chan Try[A], DefaultBufferSize)
	tokens := make(chan struct{}, burst)

	// Fill the token bucket initially
	for i := 0; i < burst; i++ {
		tokens <- struct{}{}
	}

	// Refill tokens at the specified rate
	refillDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(rate))
		defer ticker.Stop()
		defer close(refillDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-refillDone:
				return
			case <-ticker.C:
				// Try to add a token, but don't block if bucket is full
				select {
				case tokens <- struct{}{}:
				case <-ctx.Done():
					return
				case <-refillDone:
					return
				default:
					// Bucket is full, skip
				}
			}
		}
	}()

	// Process the input stream
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				// Wait for a token
				select {
				case <-ctx.Done():
					return
				case <-tokens:
					out <- item
				}
			}
		}
	}()

	return out
}

func Debounce[A any](ctx context.Context, in <-chan Try[A], delay time.Duration) <-chan Try[A] {
	if ctx == nil {
		ctx = context.Background()
	}

	out := make(chan Try[A])

	go func() {
		defer close(out)

		var pending Try[A]
		var pendingOk bool
		timer := time.NewTimer(delay)
		stopped := timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case item, ok := <-in:
				if !ok {
					if pendingOk {
						out <- pending
					}
					return
				}
				pending = item
				pendingOk = true
				if !stopped {
					if !timer.Stop() {
						<-timer.C
					}
				}
				timer.Reset(delay)
				stopped = false

			case <-timer.C:
				stopped = true
				out <- pending
				pendingOk = false
			}
		}
	}()

	return out
}
