package persistance

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// SSTableItem represents an item in SSTable with metadata
type SSTableItem struct {
	Key   []byte
	Value []byte
	Meta  uint64
}

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
	reader     *os.File
	compressor iCompressor

	bloom      BloomFilter
	blockIndex []IndexEntry

	cache BlockCache
	mu    sync.RWMutex

	stats SSTableMeta
}

func NewSSTable(path string, bloom BloomFilter, cache BlockCache) *SSTable {
	return &SSTable{
		filePath: path,
		bloom:    bloom,
		cache:    cache,
	}
}

func (s *SSTable) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(s.filePath)
	if err != nil {
		return fmt.Errorf("failed to open SSTable file: %w", err)
	}
	s.reader = file

	// Load index
	if err := s.LoadIndex(); err != nil {
		file.Close()
		return fmt.Errorf("failed to load index: %w", err)
	}

	// Load bloom filter
	if err := s.LoadBloomFilter(); err != nil {
		file.Close()
		return fmt.Errorf("failed to load bloom filter: %w", err)
	}

	return nil
}

func (s *SSTable) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.reader != nil {
		return s.reader.Close()
	}
	return nil
}

func (s *SSTable) LoadIndex() error {
	if s.reader == nil {
		return fmt.Errorf("SSTable file not open")
	}

	// Reset to beginning
	_, err := s.reader.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to seek file: %w", err)
	}

	// Build index by scanning the file
	s.blockIndex = []IndexEntry{}
	offset := int64(0)
	blockIndex := 0

	reader := bufio.NewReader(s.reader)
	for {
		// Read key length
		keyLenBytes := make([]byte, 4)
		_, err := reader.Read(keyLenBytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read key length: %w", err)
		}
		keyLen := binary.LittleEndian.Uint32(keyLenBytes)

		// Read key
		key := make([]byte, keyLen)
		_, err = reader.Read(key)
		if err != nil {
			return fmt.Errorf("failed to read key: %w", err)
		}

		// Read value length
		valueLenBytes := make([]byte, 4)
		_, err = reader.Read(valueLenBytes)
		if err != nil {
			return fmt.Errorf("failed to read value length: %w", err)
		}
		valueLen := binary.LittleEndian.Uint32(valueLenBytes)

		// Calculate block size (key + value + metadata)
		blockSize := 4 + int(keyLen) + 4 + int(valueLen) + 8 + 8 // keyLen + key + valueLen + value + seq + meta

		// Add to index
		s.blockIndex = append(s.blockIndex, IndexEntry{
			Key:         key,
			BlockOffset: offset,
			BlockSize:   blockSize,
			BlockInd:    blockIndex,
		})

		// Skip to next entry
		_, err = reader.Discard(int(valueLen) + 8 + 8) // value + seq + meta
		if err != nil {
			return fmt.Errorf("failed to skip to next entry: %w", err)
		}

		offset += int64(blockSize)
		blockIndex++
	}

	return nil
}

func (s *SSTable) LoadBloomFilter() error {
	// For now, just create an empty bloom filter
	// In a real implementation, this would load from file
	return nil
}

func (s *SSTable) HasKey(key []byte) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.bloom != nil {
		if !s.bloom.MayContain(key) {
			return false, nil
		}
	}

	// Simple key search in file
	if s.reader == nil {
		return false, fmt.Errorf("SSTable file not open")
	}

	// Read the file and search for key
	_, err := s.reader.Seek(0, 0)
	if err != nil {
		return false, fmt.Errorf("failed to seek file: %w", err)
	}

	reader := bufio.NewReader(s.reader)
	for {
		// Read key length
		keyLenBytes := make([]byte, 4)
		_, err := reader.Read(keyLenBytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			return false, fmt.Errorf("failed to read key length: %w", err)
		}
		keyLen := binary.LittleEndian.Uint32(keyLenBytes)

		// Read key
		fileKey := make([]byte, keyLen)
		_, err = reader.Read(fileKey)
		if err != nil {
			return false, fmt.Errorf("failed to read key: %w", err)
		}

		// Compare keys
		if bytes.Equal(key, fileKey) {
			return true, nil
		}

		// Read value length
		valueLenBytes := make([]byte, 4)
		_, err = reader.Read(valueLenBytes)
		if err != nil {
			return false, fmt.Errorf("failed to read value length: %w", err)
		}
		valueLen := binary.LittleEndian.Uint32(valueLenBytes)

		// Skip value
		_, err = reader.Discard(int(valueLen))
		if err != nil {
			return false, fmt.Errorf("failed to skip value: %w", err)
		}

		// Skip sequence number and metadata
		skipBytes := make([]byte, 8+8) // 8 bytes for seq, 8 for meta
		_, err = reader.Read(skipBytes)
		if err != nil {
			return false, fmt.Errorf("failed to skip metadata: %w", err)
		}
	}

	return false, nil
}

func (s *SSTable) Get(key []byte) (*SSTableItem, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.bloom != nil {
		if !s.bloom.MayContain(key) {
			return nil, fmt.Errorf("key not found")
		}
	}

	// Simple key search in file
	if s.reader == nil {
		return nil, fmt.Errorf("SSTable file not open")
	}

	// Read the file and search for key
	_, err := s.reader.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek file: %w", err)
	}

	reader := bufio.NewReader(s.reader)
	for {
		// Read key length
		keyLenBytes := make([]byte, 4)
		_, err := reader.Read(keyLenBytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read key length: %w", err)
		}
		keyLen := binary.LittleEndian.Uint32(keyLenBytes)

		// Read key
		fileKey := make([]byte, keyLen)
		_, err = reader.Read(fileKey)
		if err != nil {
			return nil, fmt.Errorf("failed to read key: %w", err)
		}

		// Read value length
		valueLenBytes := make([]byte, 4)
		_, err = reader.Read(valueLenBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read value length: %w", err)
		}
		valueLen := binary.LittleEndian.Uint32(valueLenBytes)

		// Read value
		value := make([]byte, valueLen)
		_, err = reader.Read(value)
		if err != nil {
			return nil, fmt.Errorf("failed to read value: %w", err)
		}

		// Read sequence number
		seqBytes := make([]byte, 8)
		_, err = reader.Read(seqBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read sequence: %w", err)
		}

		// Read metadata
		metaBytes := make([]byte, 8)
		_, err = reader.Read(metaBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read metadata: %w", err)
		}
		meta := binary.LittleEndian.Uint64(metaBytes)

		// Compare keys
		if bytes.Equal(key, fileKey) {
			return &SSTableItem{
				Key:   fileKey,
				Value: value,
				Meta:  meta,
			}, nil
		}
	}

	return nil, fmt.Errorf("key not found")
}

// Iterator creates an iterator for the SSTable
func (s *SSTable) Iterator() *SSTableIterator {
	return &SSTableIterator{
		sstable: s,
		reader:  s.reader,
	}
}

// NewIterator creates a new iterator
func (s *SSTable) NewIterator() *SSTableIterator {
	return &SSTableIterator{
		sstable: s,
		reader:  s.reader,
	}
}

// ApproximateSize returns the approximate size of the SSTable
func (s *SSTable) ApproximateSize() int64 {
	if s.reader == nil {
		return 0
	}

	fileInfo, err := s.reader.Stat()
	if err != nil {
		return 0
	}

	return fileInfo.Size()
}

// GetFilePath returns the file path of the SSTable
func (s *SSTable) GetFilePath() string {
	return s.filePath
}

// SSTableIterator iterates over SSTable entries
type SSTableIterator struct {
	sstable *SSTable
	reader  *os.File
	key     []byte
	value   []byte
	meta    uint64
	err     error
}

// First moves to the first entry
func (it *SSTableIterator) First() {
	it.reader.Seek(0, 0)
	it.Next()
}

// Next moves to the next entry
func (it *SSTableIterator) Next() {
	if it.reader == nil {
		it.err = fmt.Errorf("reader not available")
		return
	}

	// Read key length
	keyLenBytes := make([]byte, 4)
	_, err := it.reader.Read(keyLenBytes)
	if err != nil {
		if err == io.EOF {
			it.key = nil
			it.value = nil
			return
		}
		it.err = err
		return
	}
	keyLen := binary.LittleEndian.Uint32(keyLenBytes)

	// Read key
	it.key = make([]byte, keyLen)
	_, err = it.reader.Read(it.key)
	if err != nil {
		it.err = err
		return
	}

	// Read value length
	valueLenBytes := make([]byte, 4)
	_, err = it.reader.Read(valueLenBytes)
	if err != nil {
		it.err = err
		return
	}
	valueLen := binary.LittleEndian.Uint32(valueLenBytes)

	// Read value
	it.value = make([]byte, valueLen)
	_, err = it.reader.Read(it.value)
	if err != nil {
		it.err = err
		return
	}

	// Read sequence number
	seqBytes := make([]byte, 8)
	_, err = it.reader.Read(seqBytes)
	if err != nil {
		it.err = err
		return
	}

	// Read metadata
	metaBytes := make([]byte, 8)
	_, err = it.reader.Read(metaBytes)
	if err != nil {
		it.err = err
		return
	}
	it.meta = binary.LittleEndian.Uint64(metaBytes)
}

// Valid checks if the iterator is valid
func (it *SSTableIterator) Valid() bool {
	return it.key != nil && it.err == nil
}

// Key returns the current key
func (it *SSTableIterator) Key() []byte {
	return it.key
}

// Value returns the current value
func (it *SSTableIterator) Value() []byte {
	return it.value
}

// Meta returns the current metadata
func (it *SSTableIterator) Meta() uint64 {
	return it.meta
}

// Close closes the iterator
func (it *SSTableIterator) Close() error {
	return nil
}
