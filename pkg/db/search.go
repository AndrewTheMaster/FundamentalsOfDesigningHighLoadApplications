package db

import (
	"context"
	"lsmdb/pkg/iterator"
	"lsmdb/pkg/types"
)

// InternalSearchIterator wraps the low-level iterator for internal use.
type InternalSearchIterator interface {
	iterator.Iterator
}

// SearchEngine provides internal search capabilities using iterators.
type SearchEngine interface {
	NewIterator(ctx context.Context, opts ReadOptions) (InternalSearchIterator, error)
}

// SearchRange performs a range search using internal iterators.
func SearchRange(ctx context.Context, engine SearchEngine, start, end types.Key, opts SearchOptions, callback SearchCallback) error {
	iter, err := engine.NewIterator(ctx, opts.ReadOptions)
	if err != nil {
		return err
	}
	defer iter.Close()

	if opts.Reverse {
		iter.Last()
	} else {
		iter.First()
	}

	count := 0
	for iter.Valid() && (opts.Limit == 0 || count < opts.Limit) {
		key := iter.Key()
		
		// Check range bounds
		if start != nil && compareKeys(key, start) < 0 {
			if opts.Reverse {
				break
			}
			iter.Next()
			continue
		}
		if end != nil && compareKeys(key, end) > 0 {
			if opts.Reverse {
				iter.Prev()
				continue
			}
			break
		}

		// Call callback
		result := SearchResult{
			Key:   key,
			Value: iter.Value(),
		}
		if err := callback(result); err != nil {
			return err
		}

		count++
		if opts.Reverse {
			iter.Prev()
		} else {
			iter.Next()
		}
	}

	return nil
}

// compareKeys compares two keys lexicographically.
func compareKeys(a, b types.Key) int {
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	for i := 0; i < len(a); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// hasPrefix checks if key has the given prefix.
func hasPrefix(key, prefix types.Key) bool {
	if len(prefix) > len(key) {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		if key[i] != prefix[i] {
			return false
		}
	}
	return true
}
