package rill

import (
	"context"
	"sync"
)

func Merge[A any](ins ...<-chan Try[A]) <-chan Try[A] {
	return MergeWithContext(context.Background(), ins...)
}

func MergeWithContext[A any](ctx context.Context, ins ...<-chan Try[A]) <-chan Try[A] {
	if ctx == nil {
		ctx = context.Background()
	}

	switch len(ins) {
	case 0:
		return nil
	case 1:
		return ins[0]
	case 2, 3, 4, 5:
		return fastMerge(ctx, ins)
	default:
		return slowMerge(ctx, ins)
	}
}

func fastMerge[A any](ctx context.Context, ins []<-chan Try[A]) <-chan Try[A] {
	remaining := len(ins)
	for len(ins) < 5 {
		ins = append(ins, nil)
	}

	out := make(chan Try[A])

	go func() {
		defer close(out)

		var a Try[A]
		var ok bool
		var i int

		for {
			if remaining == 0 {
				return
			}

			select {
			case <-ctx.Done():
				return
			case a, ok = <-ins[0]:
				i = 0
			case a, ok = <-ins[1]:
				i = 1
			case a, ok = <-ins[2]:
				i = 2
			case a, ok = <-ins[3]:
				i = 3
			case a, ok = <-ins[4]:
				i = 4
			}

			if !ok {
				remaining--
				ins[i] = nil
				continue
			}

			out <- a
		}
	}()

	return out
}

func slowMerge[A any](ctx context.Context, ins []<-chan Try[A]) <-chan Try[A] {
	out := make(chan Try[A])

	var wg sync.WaitGroup
	for _, in := range ins {
		in1 := in
		wg.Add(1)
		go func() {
			defer wg.Done()
			for x := range in1 {
				select {
				case <-ctx.Done():
					return
				case out <- x:
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

func Concat[A any](ins ...<-chan Try[A]) <-chan Try[A] {
	return ConcatWithContext(context.Background(), ins...)
}

func ConcatWithContext[A any](ctx context.Context, ins ...<-chan Try[A]) <-chan Try[A] {
	if ctx == nil {
		ctx = context.Background()
	}

	if len(ins) == 0 {
		return nil
	}

	out := make(chan Try[A])

	go func() {
		defer close(out)
	inner:
		for _, in := range ins {
			for {
				select {
				case <-ctx.Done():
					return
				case x, ok := <-in:
					if !ok {
						continue inner
					}
					out <- x
				}
			}
		}
	}()

	return out
}

func Zip[A, B any](ctx context.Context, inA <-chan Try[A], inB <-chan Try[B]) <-chan Try[[2]any] {
	if ctx == nil {
		ctx = context.Background()
	}

	if inA == nil || inB == nil {
		return nil
	}

	out := make(chan Try[[2]any])

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case a, ok := <-inA:
				if !ok {
					Discard(inB)
					return
				}
				if a.Error != nil {
					out <- Try[[2]any]{Error: a.Error}
					Discard(inB)
					return
				}
				b, ok := <-inB
				if !ok {
					return
				}
				if b.Error != nil {
					out <- Try[[2]any]{Error: b.Error}
					return
				}
				out <- Try[[2]any]{Value: [2]any{a.Value, b.Value}}
			case b, ok := <-inB:
				if !ok {
					Discard(inA)
					return
				}
				if b.Error != nil {
					out <- Try[[2]any]{Error: b.Error}
					Discard(inA)
					return
				}
				a, ok := <-inA
				if !ok {
					return
				}
				if a.Error != nil {
					out <- Try[[2]any]{Error: a.Error}
					return
				}
				out <- Try[[2]any]{Value: [2]any{a.Value, b.Value}}
			}
		}
	}()

	return out
}
