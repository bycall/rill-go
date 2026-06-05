package rill

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWrap(t *testing.T) {
	val := Wrap(42, nil)
	if val.Value != 42 || val.Error != nil {
		t.Error("Wrap with no error failed")
	}

	err := errors.New("test error")
	val = Wrap(0, err)
	if val.Error != err {
		t.Error("Wrap with error failed")
	}
}

func TestFromSlice(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5}
	stream := FromSlice(slice, nil)

	result, err := ToSlice(stream)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 5 {
		t.Errorf("Expected 5 items, got %d", len(result))
	}
	for i, v := range result {
		if v != slice[i] {
			t.Errorf("Expected %d, got %d", slice[i], v)
		}
	}
}

func TestMap(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3}, nil)
	resultStream := Map(stream, 2, func(x int) (int, error) {
		return x * 2, nil
	})

	result, err := ToSlice(resultStream)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 items, got %d", len(result))
	}
}

func TestFilter(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3, 4, 5}, nil)
	resultStream := Filter(stream, 2, func(x int) (bool, error) {
		return x%2 == 0, nil
	})

	result, err := ToSlice(resultStream)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 2 {
		t.Errorf("Expected 2 items, got %d", len(result))
	}
}

func TestForEach(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3}, nil)
	var sum int
	var mu sync.Mutex
	err := ForEach(stream, 2, func(x int) error {
		mu.Lock()
		defer mu.Unlock()
		sum += x
		return nil
	})
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if sum != 6 {
		t.Errorf("Expected sum 6, got %d", sum)
	}
}

func TestErr(t *testing.T) {
	errExpected := errors.New("test error")
	var nilSlice []int
	stream := FromSlice(nilSlice, errExpected)
	err := Err(stream)
	if err != errExpected {
		t.Error("Expected error not returned")
	}
}

func TestFirst(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3}, nil)
	val, found, err := First(stream)
	if !found || err != nil || val != 1 {
		t.Error("First() failed")
	}
}

func TestAny(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3}, nil)
	found, err := Any(stream, 2, func(x int) (bool, error) {
		return x == 2, nil
	})
	if err != nil || !found {
		t.Error("Any() should find item")
	}
}

func TestAll(t *testing.T) {
	stream := FromSlice([]int{2, 4, 6}, nil)
	all, err := All(stream, 2, func(x int) (bool, error) {
		return x%2 == 0, nil
	})
	if err != nil || !all {
		t.Error("All() should return true")
	}
}

func TestBatch(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3, 4, 5, 6}, nil)
	batched := Batch(stream, 2, -1*time.Second)
	result, err := ToSlice(batched)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 batches, got %d", len(result))
	}
}

func TestUnbatch(t *testing.T) {
	batches := [][]int{{1, 2}, {3, 4}}
	stream := FromSlice(batches, nil)
	unbatched := Unbatch(stream)
	result, err := ToSlice(unbatched)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 4 {
		t.Errorf("Expected 4 items, got %d", len(result))
	}
}

func TestReduce(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3, 4, 5}, nil)
	result, found, err := Reduce(stream, 2, func(a, b int) (int, error) {
		return a + b, nil
	})
	if err != nil || !found || result != 15 {
		t.Errorf("Reduce failed: got %d, expected 15", result)
	}
}

func TestCount(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3, 4, 5}, nil)
	count, err := Count(stream)
	if err != nil || count != 5 {
		t.Errorf("Count failed: got %d, expected 5", count)
	}
}

func TestTake(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3, 4, 5}, nil)
	taken := Take(stream, 3)
	result, err := ToSlice(taken)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 items, got %d", len(result))
	}
}

func TestSkip(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3, 4, 5}, nil)
	skipped := Skip(stream, 2)
	result, err := ToSlice(skipped)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 items, got %d", len(result))
	}
}

func TestDistinct(t *testing.T) {
	stream := FromSlice([]int{1, 2, 2, 3, 3, 3}, nil)
	distinct := Distinct(stream)
	result, err := ToSlice(distinct)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 distinct items, got %d", len(result))
	}
}

func TestMerge(t *testing.T) {
	stream1 := FromSlice([]int{1, 2, 3}, nil)
	stream2 := FromSlice([]int{4, 5, 6}, nil)
	merged := Merge(stream1, stream2)
	result, err := ToSlice(merged)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 6 {
		t.Errorf("Expected 6 items, got %d", len(result))
	}
}

func TestConcat(t *testing.T) {
	stream1 := FromSlice([]int{1, 2, 3}, nil)
	stream2 := FromSlice([]int{4, 5, 6}, nil)
	concatenated := Concat(stream1, stream2)
	result, err := ToSlice(concatenated)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 6 {
		t.Errorf("Expected 6 items, got %d", len(result))
	}
}

func TestOrderedFlatMap(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3}, nil)
	resultStream := OrderedFlatMap(stream, 2, func(x int) <-chan Try[int] {
		return FromSlice([]int{x, x * 2}, nil)
	})
	result, err := ToSlice(resultStream)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	expected := []int{1, 2, 2, 4, 3, 6}
	if len(result) != len(expected) {
		t.Errorf("Expected %d items, got %d", len(expected), len(result))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("Expected %d at index %d, got %d", expected[i], i, v)
		}
	}
}

func TestOrElse(t *testing.T) {
	// Test case with error in primary stream
	errStream := make(chan Try[int], 1)
	errStream <- Try[int]{Error: errors.New("test error")}
	close(errStream)

	fallbackStream := FromSlice([]int{10, 20}, nil)
	orElse := OrElse(errStream, fallbackStream)
	result, err := ToSlice(orElse)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 2 {
		t.Errorf("Expected 2 items, got %d", len(result))
	}
	if result[0] != 10 || result[1] != 20 {
		t.Errorf("Expected [10,20], got %v", result)
	}

	// Test case with no error in primary stream
	primaryStream := FromSlice([]int{1, 2, 3}, nil)
	orElse = OrElse(primaryStream, fallbackStream)
	result, err = ToSlice(orElse)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 items, got %d", len(result))
	}
}

func TestCatchWithFallback(t *testing.T) {
	stream := make(chan Try[int], 3)
	stream <- Try[int]{Value: 1}
	stream <- Try[int]{Error: errors.New("test error")}
	stream <- Try[int]{Value: 3}
	close(stream)

	catchStream := CatchWithFallback(stream, 1, -1, func(err error) error {
		return nil
	})
	result, err := ToSlice(catchStream)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 items, got %d", len(result))
	}
	if result[0] != 1 || result[1] != -1 || result[2] != 3 {
		t.Errorf("Expected [1, -1, 3], got %v", result)
	}
}

func TestFanOutFanIn(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3, 4, 5, 6}, nil)
	fanned := FanOut(nil, stream, 2)
	if len(fanned) != 2 {
		t.Errorf("Expected 2 channels, got %d", len(fanned))
	}

	// Fan in
	fannedIn := FanIn(nil, fanned...)
	result, err := ToSlice(fannedIn)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 6 {
		t.Errorf("Expected 6 items, got %d", len(result))
	}
}

func TestThrottle(t *testing.T) {
	stream := FromSlice([]int{1, 2, 3}, nil)
	throttled := Throttle(nil, stream, 10, 2) // 10 per second, burst 2
	result, err := ToSlice(throttled)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 items, got %d", len(result))
	}
}

func TestMetrics(t *testing.T) {
	// Reset metrics
	globalMetrics.Reset()

	stream := FromSlice([]int{1, 2, 3}, nil)
	resultStream := Map(stream, 2, func(x int) (int, error) {
		return x * 2, nil
	})
	ToSlice(resultStream)

	snapshot := globalMetrics.Snapshot()
	if snapshot.ProcessedItems != 3 {
		t.Errorf("Expected 3 processed items, got %d", snapshot.ProcessedItems)
	}
	if snapshot.ActiveWorkers != 0 {
		t.Errorf("Expected 0 active workers after processing, got %d", snapshot.ActiveWorkers)
	}
}

func TestTimeout(t *testing.T) {
	// Test with sufficient timeout
	stream := FromSlice([]int{1, 2, 3}, nil)
	timeoutStream := Timeout(stream, 5*time.Second)
	result, err := ToSlice(timeoutStream)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 items, got %d", len(result))
	}

	// Test with zero/negative timeout (should pass through)
	stream2 := FromSlice([]int{1, 2, 3}, nil)
	timeoutStream = Timeout(stream2, 0)
	result, err = ToSlice(timeoutStream)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 items with zero timeout, got %d", len(result))
	}
}

func TestRetry(t *testing.T) {
	// Test retry with failing function
	callCount := 0
	stream := FromSlice([]int{1, 2}, nil)
	retryStream := Retry(context.Background(), stream, DefaultRetryOptions(), func(ctx context.Context, x int) (int, error) {
		callCount++
		if callCount < 3 {
			return 0, errors.New("temporary error")
		}
		return x * 2, nil
	})
	result, err := ToSlice(retryStream)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 2 {
		t.Errorf("Expected 2 items, got %d", len(result))
	}
}

func TestWindow(t *testing.T) {
	// Test CountWindow - it returns []Try, not Try, so we need a custom collector
	stream := FromSlice([]int{1, 2, 3, 4, 5}, nil)
	windowed := CountWindow(stream, 2)

	count := 0
	for w := range windowed {
		count++
		_ = w // window slice
	}
	if count != 3 { // [1,2], [3,4], [5] = 3 windows
		t.Errorf("Expected 3 windows, got %d", count)
	}
}

func TestDebounce(t *testing.T) {
	// Test basic debounce
	stream := FromSlice([]int{1, 2, 3}, nil)
	debounced := Debounce(context.Background(), stream, 100*time.Millisecond)
	result, err := ToSlice(debounced)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(result) != 1 { // Only the last item should remain
		t.Errorf("Expected 1 item after debounce, got %d", len(result))
	}
}

func BenchmarkMap(b *testing.B) {
	items := make([]int, 10000)
	for i := range items {
		items[i] = i
	}
	stream := FromSlice(items, nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		resultStream := Map(stream, 4, func(x int) (int, error) {
			return x * 2, nil
		})
		Discard(resultStream)
	}
}

func BenchmarkFilter(b *testing.B) {
	items := make([]int, 10000)
	for i := range items {
		items[i] = i
	}
	stream := FromSlice(items, nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		resultStream := Filter(stream, 4, func(x int) (bool, error) {
			return x%2 == 0, nil
		})
		Discard(resultStream)
	}
}

func BenchmarkFlatMap(b *testing.B) {
	items := make([]int, 1000)
	for i := range items {
		items[i] = i
	}
	stream := FromSlice(items, nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		resultStream := FlatMap(stream, 4, func(x int) <-chan Try[int] {
			out := make(chan Try[int], 2)
			out <- Try[int]{Value: x}
			out <- Try[int]{Value: x}
			close(out)
			return out
		})
		Discard(resultStream)
	}
}

func BenchmarkPipelineChain(b *testing.B) {
	items := make([]int, 5000)
	for i := range items {
		items[i] = i
	}
	stream := FromSlice(items, nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		resultStream := Filter(stream, 4, func(x int) (bool, error) {
			return x%2 == 0, nil
		})
		resultStream = Map(resultStream, 4, func(x int) (int, error) {
			return x * 2, nil
		})
		resultStream = Map(resultStream, 4, func(x int) (int, error) {
			return x + 1, nil
		})
		Discard(resultStream)
	}
}

func BenchmarkFromSliceToSlice(b *testing.B) {
	items := make([]int, 10000)
	for i := range items {
		items[i] = i
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stream := FromSlice(items, nil)
		Discard(stream)
	}
}

func BenchmarkOrderedMap(b *testing.B) {
	items := make([]int, 1000)
	for i := range items {
		items[i] = i
	}
	stream := FromSlice(items, nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		resultStream := OrderedMap(stream, 4, func(x int) (int, error) {
			return x * 2, nil
		})
		Discard(resultStream)
	}
}
