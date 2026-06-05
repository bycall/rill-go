package rill

import (
	"context"
	"fmt"
)

type Try[A any] struct {
	Value A
	Error error
}

func (t Try[A]) IsError() bool {
	return t.Error != nil
}

func (t Try[A]) IsOk() bool {
	return t.Error == nil
}

func Wrap[A any](value A, err error) Try[A] {
	return Try[A]{Value: value, Error: err}
}

func FromSlice[A any](slice []A, err error) <-chan Try[A] {
	const maxBufferSize = 512

	if err != nil {
		out := make(chan Try[A], 1)
		out <- Try[A]{Error: err}
		close(out)
		return out
	}

	sendAll := func(in []A, out chan Try[A]) {
		for _, a := range in {
			out <- Try[A]{Value: a}
		}
		close(out)
	}

	if len(slice) <= maxBufferSize {
		out := make(chan Try[A], len(slice))
		sendAll(slice, out)
		return out
	}

	out := make(chan Try[A], maxBufferSize)
	go sendAll(slice, out)
	return out
}

func ToSlice[A any](in <-chan Try[A]) ([]A, error) {
	var res []A
	for x := range in {
		if err := x.Error; err != nil {
			Discard(in)
			return res, err
		}
		res = append(res, x.Value)
	}
	return res, nil
}

func FromChan[A any](values <-chan A, err error) <-chan Try[A] {
	if values == nil && err == nil {
		return nil
	}

	out := make(chan Try[A])
	go func() {
		defer close(out)
		if err != nil {
			out <- Try[A]{Error: err}
		}
		for x := range values {
			out <- Try[A]{Value: x}
		}
	}()
	return out
}

func FromChans[A any](values <-chan A, errs <-chan error) <-chan Try[A] {
	if values == nil && errs == nil {
		return nil
	}

	out := make(chan Try[A])

	go func() {
		defer close(out)
		for {
			select {
			case err, ok := <-errs:
				if ok {
					if err != nil {
						out <- Try[A]{Error: err}
					}
				} else {
					errs = nil
					if values == nil && errs == nil {
						return
					}
				}
			case v, ok := <-values:
				if ok {
					out <- Try[A]{Value: v}
				} else {
					values = nil
					if values == nil && errs == nil {
						return
					}
				}
			}
		}
	}()
	return out
}

func ToChans[A any](in <-chan Try[A]) (<-chan A, <-chan error) {
	if in == nil {
		return nil, nil
	}

	out := make(chan A)
	errs := make(chan error)

	go func() {
		defer close(out)
		defer close(errs)
		for x := range in {
			if x.Error != nil {
				errs <- x.Error
			} else {
				out <- x.Value
			}
		}
	}()

	return out, errs
}

func Generate[A any](f func(send func(A), sendErr func(error))) <-chan Try[A] {
	out := make(chan Try[A])
	go func() {
		defer close(out)
		send := func(a A) {
			out <- Try[A]{Value: a}
		}
		sendErr := func(err error) {
			out <- Try[A]{Error: err}
		}
		f(send, sendErr)
	}()
	return out
}

func Discard[A any](in <-chan A) {
	go drain(in)
}

func drain[A any](in <-chan A) {
	for range in {
	}
}

type Stream[A any] <-chan Try[A]

// PipelineBuilder provides a fluent API for building data processing pipelines.
// Note: Due to Go generics limitations, this builder uses Try[any] internally.
// Callers should use type assertions on the output. Prefer standalone functions
// (Map, Filter, FlatMap, etc.) for compile-time type safety.
type PipelineBuilder struct {
	ctx     context.Context
	stages  []func(context.Context, <-chan Try[any]) <-chan Try[any]
	input   Stream[any] // any input type, stored as Try[any]
	workers int
}

// NewPipeline creates a new PipelineBuilder from a source stream.
func NewPipeline[A any](ctx context.Context, source Stream[A]) *PipelineBuilder {
	if ctx == nil {
		ctx = context.Background()
	}
	return &PipelineBuilder{
		ctx:     ctx,
		input:   convertToAnyStream(source),
		workers: DefaultMaxWorkers,
	}
}

// convertToAnyStream converts Stream[A] to Stream[any]
func convertToAnyStream[A any](source Stream[A]) Stream[any] {
	out := make(chan Try[any], DefaultBufferSize)
	go func() {
		defer close(out)
		for item := range source {
			out <- Try[any]{Value: item.Value, Error: item.Error}
		}
	}()
	return out
}

func (p *PipelineBuilder) WithWorkers(n int) *PipelineBuilder {
	p.workers = n
	return p
}

// Map transforms each element using the provided function.
func (p *PipelineBuilder) Map(f func(context.Context, any) (any, error)) *PipelineBuilder {
	p.stages = append(p.stages, func(ctx context.Context, in <-chan Try[any]) <-chan Try[any] {
		out := make(chan Try[any], DefaultBufferSize)
		go func() {
			defer close(out)
			for item := range in {
				if item.IsError() {
					out <- item
					continue
				}
				result, err := f(ctx, item.Value)
				out <- Try[any]{Value: result, Error: err}
			}
		}()
		return out
	})
	return p
}

// Filter keeps elements that satisfy the predicate.
func (p *PipelineBuilder) Filter(f func(context.Context, any) (bool, error)) *PipelineBuilder {
	fn := f
	p.stages = append(p.stages, func(ctx context.Context, in <-chan Try[any]) <-chan Try[any] {
		out := make(chan Try[any], DefaultBufferSize)
		go func() {
			defer close(out)
			for item := range in {
				if item.IsError() {
					out <- item
					continue
				}
				keep, err := fn(ctx, item.Value)
				if err != nil {
					out <- item
					continue
				}
				if keep {
					out <- item
				}
			}
		}()
		return out
	})
	return p
}

// FlatMap transforms each element into multiple elements.
func (p *PipelineBuilder) FlatMap(f func(context.Context, any) ([]any, error)) *PipelineBuilder {
	fn := f
	p.stages = append(p.stages, func(ctx context.Context, in <-chan Try[any]) <-chan Try[any] {
		out := make(chan Try[any], DefaultBufferSize)
		go func() {
			defer close(out)
			for item := range in {
				if item.IsError() {
					out <- item
					continue
				}
				results, err := fn(ctx, item.Value)
				if err != nil {
					out <- Try[any]{Error: err}
					continue
				}
				for _, r := range results {
					out <- Try[any]{Value: r}
				}
			}
		}()
		return out
	})
	return p
}

// FilterMap transforms and filters elements.
func (p *PipelineBuilder) FilterMap(f func(context.Context, any) (any, bool, error)) *PipelineBuilder {
	fn := f
	p.stages = append(p.stages, func(ctx context.Context, in <-chan Try[any]) <-chan Try[any] {
		out := make(chan Try[any], DefaultBufferSize)
		go func() {
			defer close(out)
			for item := range in {
				if item.IsError() {
					out <- item
					continue
				}
				result, keep, err := fn(ctx, item.Value)
				if err != nil {
					out <- Try[any]{Error: err}
					continue
				}
				if keep {
					out <- Try[any]{Value: result}
				}
			}
		}()
		return out
	})
	return p
}

// ForEach executes a function for each element and forwards results.
func (p *PipelineBuilder) ForEach(f func(context.Context, any) error) *PipelineBuilder {
	p.stages = append(p.stages, func(ctx context.Context, in <-chan Try[any]) <-chan Try[any] {
		out := make(chan Try[any], DefaultBufferSize)
		go func() {
			defer close(out)
			for item := range in {
				if item.IsError() {
					out <- item
					continue
				}
				if err := f(ctx, item.Value); err != nil {
					out <- Try[any]{Error: err}
					continue
				}
				out <- item
			}
		}()
		return out
	})
	return p
}

// Batch groups elements into batches of the specified size.
func (p *PipelineBuilder) Batch(size int) *PipelineBuilder {
	batchSize := size
	p.stages = append(p.stages, func(ctx context.Context, in <-chan Try[any]) <-chan Try[any] {
		out := make(chan Try[any], DefaultBufferSize)
		go func() {
			defer close(out)
			batch := make([]any, 0, batchSize)
			for item := range in {
				if item.IsError() {
					if len(batch) > 0 {
						out <- Try[any]{Value: batch}
						batch = make([]any, 0, batchSize)
					}
					out <- item
					continue
				}
				batch = append(batch, item.Value)
				if len(batch) >= batchSize {
					out <- Try[any]{Value: batch}
					batch = make([]any, 0, batchSize)
				}
			}
			if len(batch) > 0 {
				out <- Try[any]{Value: batch}
			}
		}()
		return out
	})
	return p
}

// Build executes the pipeline and returns the output stream.
func (p *PipelineBuilder) Build() Stream[any] {
	current := p.input
	for _, stage := range p.stages {
		current = Stream[any](stage(p.ctx, current))
	}
	return current
}

// Run executes the pipeline and discards all results.
func (p *PipelineBuilder) Run() error {
	for item := range p.Build() {
		if item.IsError() {
			return item.Error
		}
	}
	return nil
}

func ParallelMap[A, B any](ctx context.Context, items []A, workers int, fn func(context.Context, A) (B, error)) ([]B, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	input := FromSlice(items, nil)
	output := MapWithContext(ctx, input, workers, fn)

	return ToSlice(output)
}

func ParallelProcess[A any](ctx context.Context, items []A, workers int, fn func(context.Context, A) error) error {
	if ctx == nil {
		ctx = context.Background()
	}

	input := FromSlice(items, nil)
	return ForEachWithContext(ctx, input, workers, fn)
}

func Drain[A any](ctx context.Context, in <-chan Try[A]) error {
	var lastErr error
	for item := range in {
		if item.Error != nil {
			lastErr = item.Error
		}
	}
	return lastErr
}

func Must[A any](result Try[A], err error) A {
	if err != nil {
		panic(fmt.Sprintf("rill.Must: %v", err))
	}
	if result.Error != nil {
		panic(fmt.Sprintf("rill.Must: %v", result.Error))
	}
	return result.Value
}

func MustOk[A any](result Try[A], err error) (A, error) {
	if err != nil {
		return result.Value, err
	}
	return result.Value, result.Error
}
