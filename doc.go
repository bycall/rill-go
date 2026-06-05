// Package rill provides a toolkit for composable concurrency in Go,
// making it easier to build concurrent programs from simple, reusable parts.
//
// Rill reduces boilerplate while preserving Go's natural channel-based model
// and backpressure behavior. It provides functions for transforming, filtering,
// batching, and reducing streams of data with full control over concurrency.
//
// Key features:
//   - Map, Filter, FlatMap with configurable concurrency
//   - Ordered versions of all transformation functions
//   - Batch processing with size and timeout controls
//   - Reduce and MapReduce for aggregating results
//   - Error handling through the Try container type
//
// Basic usage:
//
//	// Convert a slice to a stream
//	ids := rill.FromSlice([]int{1, 2, 3, 4, 5}, nil)
//
//	// Process concurrently with Map
//	users := rill.Map(ids, 3, func(id int) (*User, error) {
//	    return fetchUser(id)
//	})
//
//	// Consume the stream
//	err := rill.ForEach(users, 2, func(u *User) error {
//	    return saveUser(u)
//	})
package rill