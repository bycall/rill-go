package rill

func Take[A any](in <-chan Try[A], n int) <-chan Try[A] {
	if in == nil || n <= 0 {
		return nil
	}

	out := make(chan Try[A])

	go func() {
		defer close(out)
		count := 0
		for x := range in {
			if count >= n {
				Discard(in)
				return
			}
			out <- x
			count++
			if x.Error != nil {
				return
			}
		}
	}()

	return out
}

func Skip[A any](in <-chan Try[A], n int) <-chan Try[A] {
	if in == nil || n <= 0 {
		return in
	}

	out := make(chan Try[A])

	go func() {
		defer close(out)
		count := 0
		for x := range in {
			if count < n {
				count++
				continue
			}
			out <- x
			if x.Error != nil {
				return
			}
		}
	}()

	return out
}

func Distinct[A comparable](in <-chan Try[A]) <-chan Try[A] {
	return DistinctWithLimit(in, 0)
}

func DistinctWithLimit[A comparable](in <-chan Try[A], maxSize int) <-chan Try[A] {
	if in == nil {
		return nil
	}

	out := make(chan Try[A])

	go func() {
		defer close(out)

		var cache *LRUCache
		if maxSize > 0 {
			cache = NewLRUCache(maxSize)
		} else {
			cache = NewLRUCache(DefaultLRUSize)
		}

		for x := range in {
			if x.Error != nil {
				out <- x
				return
			}
			if cache.Add(x.Value) {
				out <- x
			}
		}
	}()

	return out
}

func DistinctBy[A any, K comparable](in <-chan Try[A], keyFunc func(A) K) <-chan Try[A] {
	return DistinctByWithLimit(in, keyFunc, 0)
}

func DistinctByWithLimit[A any, K comparable](in <-chan Try[A], keyFunc func(A) K, maxSize int) <-chan Try[A] {
	if in == nil {
		return nil
	}

	out := make(chan Try[A])

	go func() {
		defer close(out)

		var cache *LRUCache
		if maxSize > 0 {
			cache = NewLRUCache(maxSize)
		} else {
			cache = NewLRUCache(DefaultLRUSize)
		}

		for x := range in {
			if x.Error != nil {
				out <- x
				return
			}
			key := keyFunc(x.Value)
			if cache.Add(key) {
				out <- x
			}
		}
	}()

	return out
}

func Repeat[A any](value A, count int) <-chan Try[A] {
	if count <= 0 {
		return nil
	}

	out := make(chan Try[A], count)
	for i := 0; i < count; i++ {
		out <- Try[A]{Value: value}
	}
	close(out)
	return out
}

func Range(start, end int) <-chan Try[int] {
	if start >= end {
		return nil
	}

	out := make(chan Try[int], end-start)
	for i := start; i < end; i++ {
		out <- Try[int]{Value: i}
	}
	close(out)
	return out
}

func Tap[A any](in <-chan Try[A], f func(A)) <-chan Try[A] {
	if in == nil {
		return nil
	}

	out := make(chan Try[A])

	go func() {
		defer close(out)
		for x := range in {
			if x.Error == nil {
				f(x.Value)
			}
			out <- x
		}
	}()

	return out
}
