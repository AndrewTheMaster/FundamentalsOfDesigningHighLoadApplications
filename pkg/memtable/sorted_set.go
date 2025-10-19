package memtable

type sortedSet struct {
	*concurrentSet
}

type SortedSet interface {
	Sorted() []Item
}

func (s *sortedSet) Sorted() []Item {
	result := make([]Item, 0, s.Len())
	s.Range(func(key []byte, value Item) bool {
		result = append(result, value)
		return true
	})

	return result
}
