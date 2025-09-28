package persistance

import (
	"io"
	"sync"
	"time"
)

type iFileReader interface {
	ReadBlockAt(offset int64, size int) ([]byte, error)
	Close() error
}

type iCompressor interface {
	Unpack(r io.Reader) (io.Reader, error)
}

type BloomFilter interface {
	Add(key []byte)
	MayContain(key []byte) bool
}

type BlockCache interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte)
}

type IndexEntry struct {
	Key         []byte
	BlockOffset int64
	BlockSize   int
	BlockInd    int
}

type SSTableMeta struct {
	NumBlocks   int
	NumKeys     int
	ApproxBytes int64
	CreatedAt   time.Time
}

type SSTable struct {
	filePath   string
	reader     iFileReader
	compressor iCompressor

	bloom      BloomFilter
	blockIndex []IndexEntry

	cache BlockCache
	mu    sync.RWMutex

	stats SSTableMeta
}

func NewSSTable(path string, r iFileReader, c iCompressor, b BloomFilter) *SSTable {
	return &SSTable{filePath: path, reader: r, compressor: c, bloom: b}
}

func (s *SSTable) Open() error {
	panic("not implemented")
}

func (s *SSTable) Close() error {
	panic("not implemented")
}

func (s *SSTable) LoadIndex() error {
	panic("not implemented")
}

func (s *SSTable) LoadBloomFilter() error {
	panic("not implemented")
}

func (s *SSTable) HasKey(key []byte) (bool, error) {
	panic("not implemented")
}

func (s *SSTable) Get(key []byte) ([]byte, error) {
	panic("not implemented")
}
