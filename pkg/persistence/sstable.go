package persistence

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

// SSTableItem represents an item in SSTable with metadata
type SSTableItem struct {
	Key   []byte
	Value []byte
	ID    uint64
	Meta  uint64
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
	filePath string
	reader   *os.File

	bloom      BloomFilter
	blockIndex []IndexEntry

	cache BlockCache
	mu    sync.RWMutex
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
	// assign reader early so LoadIndex/LoadBloomFilter can use it
	s.reader = file

	// Load index
	if err := s.LoadIndex(); err != nil {
		if cerr := file.Close(); cerr != nil {
			slog.Warn("failed to close sstable file after LoadIndex error", "path", s.filePath, "error", cerr)
		}
		return fmt.Errorf("failed to load index: %w", err)
	}

	// Load bloom filter
	if err := s.LoadBloomFilter(); err != nil {
		if cerr := file.Close(); cerr != nil {
			slog.Warn("failed to close sstable file after LoadBloomFilter error", "path", s.filePath, "error", cerr)
		}
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
	const (
		sizeFieldSize = 4
		seqNumSize    = 8
		metaSize      = 8
	)

	if s.reader == nil {
		return fmt.Errorf("SSTable file not open")
	}

	// Получаем размер файла
	fileInfo, err := s.reader.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := fileInfo.Size()
	if fileSize < 4 {
		return fmt.Errorf("file too small to contain index size")
	}

	// Read index size (4 bytes at the end of the file)
	var (
		indexSize uint32
	)
	_, err = s.reader.Seek(fileSize-sizeFieldSize, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to index size: %w", err)
	}
	err = binary.Read(s.reader, binary.LittleEndian, &indexSize)
	if err != nil {
		return fmt.Errorf("failed to read index size: %w", err)
	}
	// simple index size validation
	if int64(indexSize) > fileSize-sizeFieldSize {
		return fmt.Errorf("invalid index size")
	}

	// reset file pointer to the beginning
	_, err = s.reader.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek file: %w", err)
	}

	// Read index entries until reaching indexOffsetR
	var (
		indexOffsetR = fileSize - sizeFieldSize - int64(indexSize)
		reader       = bufio.NewReader(s.reader)

		blockIndexSz, offset int64
		lenBuff              [4]byte
	)
	for offset < indexOffsetR {
		n, err := io.ReadFull(reader, lenBuff[:])
		if err != nil {
			if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("failed to read key length: %w", err)
		}
		if n != 4 {
			break
		}
		keyLen := binary.LittleEndian.Uint32(lenBuff[:])

		key := make([]byte, keyLen)
		n, err = io.ReadFull(reader, key)
		if err != nil {
			return fmt.Errorf("failed to read key: %w", err)
		}
		if n != int(keyLen) {
			break
		}

		n, err = io.ReadFull(reader, lenBuff[:])
		if err != nil {
			return fmt.Errorf("failed to read value length: %w", err)
		}
		if n != 4 {
			break
		}
		valueLen := binary.LittleEndian.Uint32(lenBuff[:])

		blockSize := 4 + int(keyLen) + 4 + int(valueLen) + 8 + 8

		s.blockIndex = append(s.blockIndex, IndexEntry{
			Key:         key,
			BlockOffset: offset,
			BlockSize:   blockSize,
			BlockInd:    int(blockIndexSz),
		})

		skip := int(valueLen) + seqNumSize + metaSize
		skipped, err := reader.Discard(skip)
		if err != nil {
			return fmt.Errorf("failed to skip to next entry: %w", err)
		}
		if skipped != skip {
			break
		}

		offset += int64(blockSize)
		blockIndexSz++
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
	if _, err := it.reader.Seek(0, 0); err != nil {
		it.err = err
		return
	}
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
