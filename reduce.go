package rill

import (
	"context"
	"sync"
)

func Reduce[A any](in <-chan Try[A], n int, f func(A, A) (A, error)) (result A, hasResult bool, err error) {
	return ReduceWithContext(context.Background(), in, n, func(ctx context.Context, a, b A) (A, error) {
		return f(a, b)
	})
}

func ReduceWithContext[A any](ctx context.Context, in <-chan Try[A], n int, f func(context.Context, A, A) (A, error)) (result A, hasResult bool, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var once OnceWithWait
	setReturns := func(result1 A, hasResult1 bool, err1 error) {
		once.Do(func() {
			result = result1
			hasResult = hasResult1
			err = err1
		})
	}

	go func() {
		var zero A

		res, ok := reduce(ctx, in, n, func(ctx context.Context, a1, a2 Try[A]) Try[A] {
			if once.WasCalled() {
				return Try[A]{}
			}
			if err := a1.Error; err != nil {
				setReturns(zero, false, err)
				return Try[A]{}
			}
			if err := a2.Error; err != nil {
				setReturns(zero, false, err)
				return Try[A]{}
			}
			res, err := f(ctx, a1.Value, a2.Value)
			if err != nil {
				setReturns(zero, false, err)
				return Try[A]{}
			}
			return Try[A]{Value: res}
		})

		if res.Error != nil {
			ok = false
		}
		setReturns(res.Value, ok, res.Error)
	}()

	once.Wait()
	return
}

func MapReduce[A any, K comparable, V any](in <-chan Try[A], nm int, mapper func(A) (K, V, error), nr int, reducer func(V, V) (V, error)) (map[K]V, error) {
	return MapReduceWithContext(context.Background(), in, nm, func(ctx context.Context, a A) (K, V, error) {
		return mapper(a)
	}, nr, func(ctx context.Context, v1, v2 V) (V, error) {
		return reducer(v1, v2)
	})
}

func MapReduceWithContext[A any, K comparable, V any](ctx context.Context, in <-chan Try[A], nm int, mapper func(context.Context, A) (K, V, error), nr int, reducer func(context.Context, V, V) (V, error)) (map[K]V, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var retMap map[K]V
	var retErr error
	var once OnceWithWait
	setReturns := func(m map[K]V, err error) {
		once.Do(func() {
			retMap = m
			retErr = err
		})
	}

	go func() {
		var zeroKey K
		var zeroVal V

		res := mapReduce(ctx, in,
			nm, func(ctx context.Context, a Try[A]) (K, V) {
				if once.WasCalled() {
					return zeroKey, zeroVal
				}
				if a.Error != nil {
					setReturns(nil, a.Error)
					return zeroKey, zeroVal
				}
				k, v, err := mapper(ctx, a.Value)
				if err != nil {
					setReturns(nil, err)
					return zeroKey, zeroVal
				}
				return k, v
			},
			nr, func(ctx context.Context, v1, v2 V) V {
				if once.WasCalled() {
					return zeroVal
				}
				res, err := reducer(ctx, v1, v2)
				if err != nil {
					setReturns(nil, err)
					return zeroVal
				}
				return res
			},
		)

		setReturns(res, nil)
	}()

	once.Wait()
	return retMap, retErr
}

func nonConcurrentReduce[A any](in <-chan A, f func(A, A) A) (A, bool) {
	res, ok := <-in
	if !ok {
		return res, false
	}
	globalMetrics.RecordChannelReceive()
	for a := range in {
		globalMetrics.RecordChannelReceive()
		res = f(res, a)
	}
	globalMetrics.RecordItem()
	return res, true
}

func reduce[A any](ctx context.Context, in <-chan A, n int, f func(context.Context, A, A) A) (A, bool) {
	if ctx == nil {
		ctx = context.Background()
	}

	if in == nil {
		var zero A
		return zero, false
	}
	if n == 1 {
		res, _ := nonConcurrentReduceWithContext(ctx, in, f)
		return res, true
	}

	currentLevel := n
	currentIn := in

	for currentLevel > 1 {
		partialResults := make(chan A, (currentLevel+1)/2)

		var wg sync.WaitGroup
		numWorkers := (currentLevel + 1) / 2

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				var a1, a2 A
				var ok1, ok2 bool

				// Non-blocking receive for first value
				select {
				case a1, ok1 = <-currentIn:
					if ok1 {
						globalMetrics.RecordChannelReceive()
					}
				case <-ctx.Done():
					return
				}

				if !ok1 {
					return
				}

				// Keep receiving and reducing until channel closes
				for {
					select {
					case <-ctx.Done():
						return
					case a2, ok2 = <-currentIn:
						if !ok2 {
							// Channel closed, send accumulated result
							partialResults <- a1
							return
						}
						globalMetrics.RecordChannelReceive()
						a1 = f(ctx, a1, a2)
					}
				}
			}()
		}

		// Wait for all workers and close output
		wg.Wait()
		close(partialResults)

		currentIn = partialResults
		currentLevel = numWorkers
	}

	return reduceSingleLevel(ctx, currentIn, f)
}

func reduceSingleLevel[A any](ctx context.Context, in <-chan A, f func(context.Context, A, A) A) (A, bool) {
	res, ok := <-in
	if !ok {
		var zero A
		return zero, false
	}
	globalMetrics.RecordChannelReceive()
	for a := range in {
		select {
		case <-ctx.Done():
			return res, false
		default:
			globalMetrics.RecordChannelReceive()
			res = f(ctx, res, a)
		}
	}
	globalMetrics.RecordItem()
	return res, true
}

func nonConcurrentReduceWithContext[A any](ctx context.Context, in <-chan A, f func(context.Context, A, A) A) (A, bool) {
	res, ok := <-in
	if !ok {
		return res, false
	}
	globalMetrics.RecordChannelReceive()
	for a := range in {
		select {
		case <-ctx.Done():
			var zero A
			return zero, false
		default:
			globalMetrics.RecordChannelReceive()
			res = f(ctx, res, a)
		}
	}
	globalMetrics.RecordItem()
	return res, true
}

type keyValue[K, V any] struct {
	Key   K
	Value V
}

func reduceIntoMap[K comparable, V any](m map[K]V, k K, v V, f func(V, V) V) {
	if oldV, ok := m[k]; ok {
		m[k] = f(oldV, v)
	} else {
		m[k] = v
	}
}

func mapReduce[A any, K comparable, V any](ctx context.Context, in <-chan A, nm int, mapper func(context.Context, A) (K, V), nr int, reducer func(context.Context, V, V) V) map[K]V {
	if ctx == nil {
		ctx = context.Background()
	}

	if in == nil {
		return nil
	}

	mapped := filterMap(in, nm, func(a A) (keyValue[K, V], bool) {
		globalMetrics.RecordChannelReceive()
		k, v := mapper(ctx, a)
		return keyValue[K, V]{k, v}, true
	})

	// Local mutex to avoid package-level contention
	var mu sync.Mutex
	reduceIntoMapLocal := func(m map[K]V, k K, v V, f func(V, V) V) {
		mu.Lock()
		defer mu.Unlock()
		if oldV, ok := m[k]; ok {
			m[k] = f(oldV, v)
		} else {
			m[k] = v
		}
	}

	if nr == 1 {
		res := make(map[K]V)
		for kv := range mapped {
			globalMetrics.RecordChannelReceive()
			reduceIntoMapLocal(res, kv.Key, kv.Value, func(v1, v2 V) V {
				return reducer(ctx, v1, v2)
			})
		}
		globalMetrics.RecordItem()
		return res
	}

	partialResults := make(chan map[K]V, nr)
	var wg sync.WaitGroup

	for i := 0; i < nr; i++ {
		wg.Add(1)
		globalMetrics.IncrementActiveWorkers()
		go func() {
			defer wg.Done()
			defer globalMetrics.DecrementActiveWorkers()
			res := make(map[K]V)
			for kv := range mapped {
				globalMetrics.RecordChannelReceive()
				reduceIntoMapLocal(res, kv.Key, kv.Value, func(v1, v2 V) V {
					return reducer(ctx, v1, v2)
				})
			}
			partialResults <- res
		}()
	}

	go func() {
		wg.Wait()
		close(partialResults)
	}()

	res := make(map[K]V)
	for m := range partialResults {
		for k, v := range m {
			reduceIntoMapLocal(res, k, v, func(v1, v2 V) V {
				return reducer(ctx, v1, v2)
			})
		}
	}
	globalMetrics.RecordItem()
	return res
}

func filterMap[A, B any](in <-chan A, n int, f func(A) (B, bool)) <-chan B {
	if in == nil {
		return nil
	}

	out := make(chan B, DefaultBufferSize)
	loop(in, out, n, func(a A) {
		b, keep := f(a)
		if keep {
			out <- b
		}
	})

	return out
}
