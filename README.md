# Rill - Composable Concurrency for Go

[![Go Report Card](https://goreportcard.com/badge/github.com/bycall/rill-go)](https://goreportcard.com/report/github.com/bycall/rill-go)
[![GoDoc](https://pkg.go.dev/badge/github.com/bycall/rill-go)](https://pkg.go.dev/github.com/bycall/rill-go)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/bycall/rill-go/blob/main/LICENSE)

Rill is a lightweight toolkit that brings composable concurrency to Go, making it easier to build concurrent programs from simple, reusable parts. It reduces boilerplate while preserving Go's natural channel-based model and backpressure behavior.

**简体中文**: Rill 是一个轻量级的 Go 并发编程工具包，使您能够从简单、可复用的组件构建并发程序。它在保留 Go 原生通道模型和背压行为的同时，减少了样板代码。

---

## Table of Contents

- [Features](#features)
- [Go Version Requirements](#go-version-requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
- [Core Concepts](#core-concepts)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)

---

## Features

| English | 中文 | Description |
|---------|------|-------------|
| **Make common tasks easier** | 简化常见任务 | Provides a cleaner and safer way of solving common concurrency problems |
| **Composable and clean** | 可组合且简洁 | Functions take Go channels as inputs and return new, transformed channels as outputs |
| **Centralized error handling** | 集中式错误处理 | Errors are automatically propagated through a pipeline |
| **Stream processing** | 流处理 | Handle potentially infinite streams, processing items as they arrive |
| **Advanced operations** | 高级操作 | Batching, ordered fan-in, map-reduce, stream splitting, merging, and more |
| **Custom extensions** | 自定义扩展 | Easy to write custom functions compatible with the library |
| **Lightweight** | 轻量级 | Small, type-safe, channel-based API with zero external dependencies |
| **Order preservation** | 顺序保持 | Choose between unordered (faster) or ordered (consistent) processing |

## Go Version Requirements

| Requirement | Version | Reason |
|-------------|---------|--------|
| **Minimum** | Go 1.18 | Uses generics (`any` type) |
| **Recommended** | Go 1.21+ | Better performance and tooling |
| **Declared** | Go 1.20 | As specified in `go.mod` |

> **Note**: This library uses Go generics extensively. Go 1.17 and earlier versions are not supported.

**简体中文**: 需要 Go 1.18 或更高版本（使用泛型特性），推荐使用 Go 1.21+。

## Installation

```bash
go get -u github.com/bycall/rill-go
```

## Quick Start

Here's a practical example demonstrating a concurrent data processing pipeline:

这是一个并发数据处理管道的实际示例：

```go
package main

import (
	"fmt"
	"time"

	"github.com/bycall/rill-go"
)

func main() {
	// Convert a slice to a stream (data source)
	ids := rill.FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

	// Process each item concurrently with 3 workers
	processed := rill.Map(ids, 3, func(id int) (int, error) {
		time.Sleep(10 * time.Millisecond)
		return id * 2, nil
	})

	// Filter results - keep only numbers greater than 10
	filtered := rill.Filter(processed, 2, func(n int) (bool, error) {
		return n > 10, nil
	})

	// Consume and print results with 2 workers
	err := rill.ForEach(filtered, 2, func(n int) error {
		fmt.Printf("Processed: %d\n", n)
		return nil
	})

	fmt.Println("Error:", err)
}
```

## Usage Examples

### Batching

Processing items in batches can significantly improve performance when working with external services or databases:

```go
import "time"

ids := rill.FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
// Batch by size 3 with 100ms timeout
batches := rill.Batch(ids, 3, 100*time.Millisecond)

err := rill.ForEach(batches, 1, func(batch []int) error {
	fmt.Printf("Processing batch: %v\n", batch)
	return nil
})
```

### Filtering

```go
numbers := rill.FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
evens := rill.Filter(numbers, 2, func(n int) (bool, error) {
	return n%2 == 0, nil
})
```

### Order Preservation

Use ordered versions when you need to maintain input order:

```go
ids := rill.FromSlice([]int{1, 2, 3, 4, 5}, nil)
processed := rill.OrderedMap(ids, 3, func(id int) (int, error) {
	return id * 2, nil
})
```

### Error Handling

Errors are automatically propagated through the pipeline:

```go
stream := rill.FromSlice([]int{1, 2, 3, 4, 5}, nil)
processed := rill.Map(stream, 2, func(n int) (int, error) {
	if n == 3 {
		return 0, fmt.Errorf("error at %d", n)
	}
	return n * 2, nil
})

err := rill.Err(processed)
```

## Core Concepts

### Streams and Try Containers

In Rill, a **stream** is a channel of `Try[A]` containers. A `Try[A]` container holds either a value of type `A` or an error:

```go
type Try[A any] struct {
	Value A
	Error error
}
```

### Non-blocking vs Blocking Functions

- **Non-blocking functions** (`Map`, `Filter`, `Batch`) take a stream as input and return a new stream immediately
- **Blocking functions** (`ForEach`, `Reduce`, `ToSlice`) consume a stream and return a final result or error

### Concurrency Control

Most functions accept a `n` parameter that controls the number of concurrent workers:

```go
// 3 concurrent workers
result := rill.Map(input, 3, mapperFunc)
```

## API Reference

### Stream Creation
- `FromSlice(slice []A, err error) Stream[A]` - Convert a slice to a stream
- `FromChan(values <-chan A, err error) Stream[A]` - Convert a channel to a stream
- `FromChans(values <-chan A, errs <-chan error) Stream[A]` - Convert two channels to a stream
- `Generate(f func(send func(A), sendErr func(error))) Stream[A]` - Create a stream from a generator

### Transformations
- `Map(in Stream[A], n int, f func(A) (B, error)) Stream[B]` - Transform each item
- `OrderedMap(in Stream[A], n int, f func(A) (B, error)) Stream[B]` - Transform preserving order
- `Filter(in Stream[A], n int, f func(A) (bool, error)) Stream[A]` - Filter items
- `OrderedFilter(in Stream[A], n int, f func(A) (bool, error)) Stream[A]` - Filter preserving order
- `FilterMap(in Stream[A], n int, f func(A) (B, bool, error)) Stream[B]` - Filter and transform
- `FlatMap(in Stream[A], n int, f func(A) Stream[B]) Stream[B]` - Transform and flatten
- `OrderedFlatMap(in Stream[A], n int, f func(A) Stream[B]) Stream[B]` - FlatMap preserving order
- `Catch(in Stream[A], n int, f func(error) error) Stream[A]` - Catch and handle errors
- `OrderedCatch(in Stream[A], n int, f func(error) error) Stream[A]` - Catch errors preserving order

### Terminal Operations
- `ForEach(in Stream[A], n int, f func(A) error) error` - Apply a function to each item
- `Err(in Stream[A]) error` - Return the first error
- `First(in Stream[A]) (value A, found bool, err error)` - Get the first item
- `Any(in Stream[A], n int, f func(A) (bool, error)) (bool, error)` - Check if any item satisfies the predicate
- `All(in Stream[A], n int, f func(A) (bool, error)) (bool, error)` - Check if all items satisfy the predicate
- `Count(in Stream[A]) (int, error)` - Count items
- `ToSlice(in Stream[A]) ([]A, error)` - Collect items into a slice

### Batching
- `Batch(in Stream[A], size int, timeout time.Duration) Stream[[]A]` - Group items into batches
- `Unbatch(in Stream[[]A]) Stream[A]` - Flatten batches into individual items

### Merging & Concatenation
- `Merge(streams ...Stream[A]) Stream[A]` - Merge multiple streams
- `Concat(streams ...Stream[A]) Stream[A]` - Concatenate multiple streams
- `Zip(in1 Stream[A], in2 Stream[B]) Stream[[2]any]` - Zip two streams together

### Additional Operations
- `Take(in Stream[A], n int) Stream[A]` - Take first n items
- `Skip(in Stream[A], n int) Stream[A]` - Skip first n items
- `Distinct(in Stream[A]) Stream[A]` - Remove duplicates
- `DistinctBy(in Stream[A], keyFunc func(A) K) Stream[A]` - Remove duplicates by key
- `Repeat(value A, count int) Stream[A]` - Repeat a value
- `Range(start, end int) Stream[int]` - Create a range stream
- `Tap(in Stream[A], f func(A)) Stream[A]` - Apply a function for side effects

### Utility Functions
- `Wrap(value A, err error) Try[A]` - Wrap a value and error
- `ToChans(in Stream[A]) (<-chan A, <-chan error)` - Split a stream into channels
- `Discard(in Stream[A])` - Discard remaining items

## Contributing

Contributions are welcome! Before submitting a pull request, please consider:

- Focus on generic, widely applicable solutions
- Keep the API surface clean and focused
- Avoid adding external dependencies
- Add tests for any new features
- For major changes, please open an issue first to discuss the approach

## Acknowledgments

This library is inspired by and builds upon the excellent work in [destel/rill](https://github.com/destel/rill). Special thanks to the original author!

## License

MIT License - see [LICENSE](LICENSE) file for details.