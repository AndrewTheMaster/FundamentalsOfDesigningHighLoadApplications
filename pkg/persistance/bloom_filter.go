package persistance

import (
	"hash"
	"hash/fnv"
)

// BloomFilterImpl implements a simple bloom filter
type BloomFilterImpl struct {
	bits     []bool
	size     uint32
	hashFunc []hash.Hash32
}

// NewBloomFilter creates a new bloom filter
func NewBloomFilter(expectedItems uint32, falsePositiveRate float64) BloomFilter {
	// Calculate optimal size and hash functions
	size := calculateOptimalSize(expectedItems, falsePositiveRate)
	hashCount := calculateHashCount(expectedItems, size)

	hashFuncs := make([]hash.Hash32, hashCount)
	for i := range hashFuncs {
		hashFuncs[i] = fnv.New32a()
	}

	return &BloomFilterImpl{
		bits:     make([]bool, size),
		size:     size,
		hashFunc: hashFuncs,
	}
}

// Add adds a key to the bloom filter
func (bf *BloomFilterImpl) Add(key []byte) {
	for i, h := range bf.hashFunc {
		h.Reset()
		h.Write(key)
		h.Write([]byte{byte(i)}) // Add salt for different hash functions
		index := h.Sum32() % bf.size
		bf.bits[index] = true
	}
}

// MayContain checks if a key might be in the bloom filter
func (bf *BloomFilterImpl) MayContain(key []byte) bool {
	for i, h := range bf.hashFunc {
		h.Reset()
		h.Write(key)
		h.Write([]byte{byte(i)}) // Add salt for different hash functions
		index := h.Sum32() % bf.size
		if !bf.bits[index] {
			return false
		}
	}
	return true
}

// calculateOptimalSize calculates the optimal size for the bloom filter
func calculateOptimalSize(expectedItems uint32, falsePositiveRate float64) uint32 {
	// m = -(n * ln(p)) / (ln(2)^2)
	// where m = size, n = expected items, p = false positive rate
	ln2 := 0.6931471805599453
	lnP := -1.0
	if falsePositiveRate > 0 {
		lnP = -1.0 * (float64(expectedItems) * -1.0 * lnP) / (ln2 * ln2)
	}
	if lnP < 1 {
		lnP = 1
	}
	return uint32(lnP)
}

// calculateHashCount calculates the optimal number of hash functions
func calculateHashCount(expectedItems uint32, size uint32) int {
	// k = (m/n) * ln(2)
	// where k = hash count, m = size, n = expected items
	ln2 := 0.6931471805599453
	k := int((float64(size) / float64(expectedItems)) * ln2)
	if k < 1 {
		k = 1
	}
	if k > 10 {
		k = 10
	}
	return k
}
