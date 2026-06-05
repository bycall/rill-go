package rill

import (
	"context"
	"fmt"
	"time"
)

func Batch[A any](in <-chan Try[A], size int, timeout time.Duration) <-chan Try[[]A] {
	return BatchWithContext(context.Background(), in, size, timeout)
}

func BatchWithContext[A any](ctx context.Context, in <-chan Try[A], size int, timeout time.Duration) <-chan Try[[]A] {
	if ctx == nil {
		ctx = context.Background()
	}
	values, errs := ToChans(in)
	batches := batch(ctx, values, size, timeout)
	return FromChans(batches, errs)
}

func Unbatch[A any](in <-chan Try[[]A]) <-chan Try[A] {
	batches, errs := ToChans(in)
	values := unbatch(batches)
	return FromChans(values, errs)
}

func batch[A any](ctx context.Context, in <-chan A, size int, timeout time.Duration) <-chan []A {
	if in == nil {
		return nil
	}

	out := make(chan []A)

	switch {
	case timeout == 0:
		panic(fmt.Errorf("zero timeout is not supported"))
	case timeout < 0:
		go func() {
			defer close(out)
			var batch []A
			for {
				select {
				case <-ctx.Done():
					if len(batch) > 0 {
						globalMetrics.RecordItem()
						out <- batch
					}
					return
				case a, ok := <-in:
					globalMetrics.RecordChannelReceive()
					if !ok {
						if len(batch) > 0 {
							globalMetrics.RecordItem()
							out <- batch
						}
						return
					}
					batch = append(batch, a)
					if len(batch) >= size {
						globalMetrics.RecordItem()
						out <- batch
						batch = make([]A, 0, size)
					}
				}
			}
		}()
	default:
		go func() {
			batch := make([]A, 0, size)
			t := time.NewTicker(1 * time.Hour)
			t.Stop()

			flush := func() {
				if len(batch) > 0 {
					globalMetrics.RecordItem()
					out <- batch
					batch = make([]A, 0, size)
				}
				t.Stop()
				select {
				case <-t.C:
				default:
				}
			}

			for {
				select {
				case <-ctx.Done():
					flush()
					close(out)
					return
				case <-t.C:
					flush()
				case a, ok := <-in:
					globalMetrics.RecordChannelReceive()
					if !ok {
						flush()
						close(out)
						return
					}
					batch = append(batch, a)
					if len(batch) == 1 {
						t.Reset(timeout)
					}
					if len(batch) >= size {
						flush()
					}
				}
			}
		}()
	}

	return out
}

func unbatch[A any](in <-chan []A) <-chan A {
	if in == nil {
		return nil
	}

	out := make(chan A)

	go func() {
		defer close(out)
		for batch := range in {
			for _, a := range batch {
				out <- a
			}
		}
	}()

	return out
}
