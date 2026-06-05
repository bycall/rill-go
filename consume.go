package rill

import (
	"context"
	"sync"
	"sync/atomic"
)

type OnceWithWait struct {
	once     sync.Once
	done     chan struct{}
	fastDone uint32
	initOnce sync.Once
}

func (o *OnceWithWait) init() {
	o.initOnce.Do(func() {
		o.done = make(chan struct{})
	})
}

func (o *OnceWithWait) Do(f func()) {
	o.once.Do(func() {
		o.init()
		f()
		atomic.StoreUint32(&o.fastDone, 1)
		close(o.done)
	})
}

func (o *OnceWithWait) Wait() {
	o.init()
	<-o.done
}

func (o *OnceWithWait) WasCalled() bool {
	return atomic.LoadUint32(&o.fastDone) > 0
}

func ForEach[A any](in <-chan Try[A], n int, f func(A) error) error {
	return ForEachWithContext(context.Background(), in, n, func(ctx context.Context, a A) error {
		return f(a)
	})
}

func ForEachWithContext[A any](ctx context.Context, in <-chan Try[A], n int, f func(context.Context, A) error) error {
	if ctx == nil {
		ctx = context.Background()
	}

	var retErr error
	var once OnceWithWait
	setReturns := func(err error) {
		once.Do(func() {
			retErr = err
		})
	}

	go func() {
		forEachWithContext(ctx, in, n, func(ctx context.Context, a Try[A]) {
			if once.WasCalled() {
				return
			}
			err := a.Error
			if err == nil {
				err = f(ctx, a.Value)
			}
			if err != nil {
				setReturns(err)
			}
		})
		setReturns(nil)
	}()

	once.Wait()
	return retErr
}

func forEachWithContext[A any](ctx context.Context, in <-chan A, n int, f func(context.Context, A)) {
	if ctx == nil {
		ctx = context.Background()
	}

	if n == 1 {
		for a := range in {
			globalMetrics.RecordChannelReceive()
			f(ctx, a)
			globalMetrics.RecordItem()
		}
		return
	}

	done := make(chan struct{})
	loopWithContext(ctx, in, done, n, func(ctx context.Context, a A) {
		globalMetrics.RecordChannelReceive()
		f(ctx, a)
		globalMetrics.RecordItem()
	})
	<-done
}

func Err[A any](in <-chan Try[A]) error {
	defer Discard(in)
	for a := range in {
		if a.Error != nil {
			return a.Error
		}
	}
	return nil
}

func First[A any](in <-chan Try[A]) (value A, found bool, err error) {
	defer Discard(in)
	for a := range in {
		return a.Value, true, a.Error
	}
	found = false
	return
}

func Any[A any](in <-chan Try[A], n int, f func(A) (bool, error)) (bool, error) {
	return AnyWithContext(context.Background(), in, n, func(ctx context.Context, a A) (bool, error) {
		return f(a)
	})
}

func AnyWithContext[A any](ctx context.Context, in <-chan Try[A], n int, f func(context.Context, A) (bool, error)) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var retFound bool
	var retErr error
	var once OnceWithWait
	setReturns := func(found bool, err error) {
		once.Do(func() {
			retFound = found
			retErr = err
		})
	}

	go func() {
		forEachWithContext(ctx, in, n, func(ctx context.Context, a Try[A]) {
			if once.WasCalled() {
				return
			}
			if err := a.Error; err != nil {
				setReturns(false, err)
				return
			}
			ok, err := f(ctx, a.Value)
			if err != nil {
				setReturns(false, err)
				return
			}
			if ok {
				setReturns(true, nil)
				return
			}
		})
		setReturns(false, nil)
	}()

	once.Wait()
	return retFound, retErr
}

func All[A any](in <-chan Try[A], n int, f func(A) (bool, error)) (bool, error) {
	res, err := AnyWithContext(context.Background(), in, n, func(ctx context.Context, a A) (bool, error) {
		ok, err := f(a)
		return !ok, err
	})
	return !res, err
}

func Count[A any](in <-chan Try[A]) (int, error) {
	count := 0
	for a := range in {
		if a.Error != nil {
			Discard(in)
			return count, a.Error
		}
		count++
	}
	return count, nil
}
