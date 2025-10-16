package memtable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// WALEntry represents a single entry in the WAL
type WALEntry struct {
	SeqNum    uint64
	Key       []byte
	Value     []byte
	Tombstone bool
}

// WAL implements write-ahead logging
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	seqNum   uint64
	filePath string
}

// NewWAL creates a new WAL instance
func NewWAL(dataDir string) (*WAL, error) {
	// Ensure directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	filePath := filepath.Join(dataDir, "wal.log")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	wal := &WAL{
		file:     file,
		writer:   bufio.NewWriter(file),
		filePath: filePath,
	}

	// Find the highest sequence number
	if err := wal.scanForLastSequence(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to scan WAL: %w", err)
	}

	return wal, nil
}

// Append adds an entry to the WAL
func (w *WAL) Append(entry WALEntry, sync bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write entry
	if err := w.writeEntry(entry); err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// Flush buffer
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}

	// Sync to disk if requested
	if sync {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL: %w", err)
		}
	}

	// Update sequence number
	if entry.SeqNum > w.seqNum {
		w.seqNum = entry.SeqNum
	}

	return nil
}

// Replay replays WAL entries
func (w *WAL) Replay(callback func(WALEntry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close current writer
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL before replay: %w", err)
	}

	// Open file for reading
	file, err := os.Open(w.filePath)
	if err != nil {
		return fmt.Errorf("failed to open WAL for reading: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		entry, err := w.readEntry(reader)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("failed to read WAL entry: %w", err)
		}

		// Call the callback
		if err := callback(entry); err != nil {
			return fmt.Errorf("WAL replay callback failed: %w", err)
		}
	}

	return nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush WAL on close: %w", err)
		}
		w.writer = nil
	}

	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("failed to close WAL file: %w", err)
		}
		w.file = nil
	}

	return nil
}

// writeEntry writes a single entry to the WAL
func (w *WAL) writeEntry(entry WALEntry) error {
	if w.writer == nil {
		return fmt.Errorf("WAL writer is nil")
	}
	
	// Write sequence number (8 bytes)
	if err := binary.Write(w.writer, binary.LittleEndian, entry.SeqNum); err != nil {
		return err
	}

	// Write tombstone flag (1 byte)
	tombstone := uint8(0)
	if entry.Tombstone {
		tombstone = 1
	}
	if err := binary.Write(w.writer, binary.LittleEndian, tombstone); err != nil {
		return err
	}

	// Write key length (4 bytes)
	keyLen := uint32(len(entry.Key))
	if err := binary.Write(w.writer, binary.LittleEndian, keyLen); err != nil {
		return err
	}

	// Write key
	if _, err := w.writer.Write(entry.Key); err != nil {
		return err
	}

	// Write value length (4 bytes)
	valueLen := uint32(len(entry.Value))
	if err := binary.Write(w.writer, binary.LittleEndian, valueLen); err != nil {
		return err
	}

	// Write value
	if _, err := w.writer.Write(entry.Value); err != nil {
		return err
	}

	return nil
}

// readEntry reads a single entry from the WAL
func (w *WAL) readEntry(reader *bufio.Reader) (WALEntry, error) {
	var entry WALEntry

	// Read sequence number (8 bytes)
	if err := binary.Read(reader, binary.LittleEndian, &entry.SeqNum); err != nil {
		return entry, err
	}

	// Read tombstone flag (1 byte)
	var tombstone uint8
	if err := binary.Read(reader, binary.LittleEndian, &tombstone); err != nil {
		return entry, err
	}
	entry.Tombstone = tombstone == 1

	// Read key length (4 bytes)
	var keyLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
		return entry, err
	}

	// Read key
	entry.Key = make([]byte, keyLen)
	if _, err := reader.Read(entry.Key); err != nil {
		return entry, err
	}

	// Read value length (4 bytes)
	var valueLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
		return entry, err
	}

	// Read value
	entry.Value = make([]byte, valueLen)
	if _, err := reader.Read(entry.Value); err != nil {
		return entry, err
	}

	return entry, nil
}

// scanForLastSequence finds the highest sequence number in the WAL file
func (w *WAL) scanForLastSequence() error {
	// Get file size
	stat, err := w.file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		w.seqNum = 0
		return nil
	}

	// Read the file to find the last sequence
	reader := bufio.NewReader(w.file)
	maxSeq := uint64(0)

	for {
		entry, err := w.readEntry(reader)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		if entry.SeqNum > maxSeq {
			maxSeq = entry.SeqNum
		}
	}

	w.seqNum = maxSeq
	return nil
}
