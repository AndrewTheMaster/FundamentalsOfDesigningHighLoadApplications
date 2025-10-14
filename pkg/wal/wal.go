package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"lsmdb/pkg/types"
	"os"
	"path/filepath"
	"sync"
)

type seqNum = uint64

// Entry represents a single entry
type Entry struct {
	SeqNum uint64
	Key    []byte
	Value  []byte
	Meta   uint64
}

// WAL implements write-ahead logging
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	seqNum   seqNum
	filePath string

	inputCh chan Entry
	doneCh  chan seqNum
}

// New creates a new WAL instance
func New(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	filePath := filepath.Join(dir, "wal.log")
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
		if err = file.Close(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("failed to scan WAL: %w", err)
	}

	return wal, nil
}

func (w *WAL) Append(entry Entry) {
	w.inputCh <- entry
}

func (w *WAL) writeFile() error {
	entry := <-w.inputCh
	if err := w.writeEntry(entry); err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	if entry.SeqNum > w.seqNum {
		w.seqNum = entry.SeqNum
	}

	// Notify completion
	w.doneCh <- entry.SeqNum

	return nil
}

func (w *WAL) Replay(start types.SeqN, callback func(Entry) error) error {
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
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read WAL entry: %w", err)
		}
		if entry.SeqNum < start {
			continue
		}

		if err := callback(entry); err != nil {
			return fmt.Errorf("WAL replay callback failed: %w", err)
		}
	}

	return nil
}

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
func (w *WAL) writeEntry(entry Entry) error {
	if w.writer == nil {
		return fmt.Errorf("WAL writer is nil")
	}

	// Write sequence number (8 bytes)
	if err := binary.Write(w.writer, binary.LittleEndian, entry.SeqNum); err != nil {
		return err
	}

	// Write metadata (8 bytes)
	if err := binary.Write(w.writer, binary.LittleEndian, entry.Meta); err != nil {
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
func (w *WAL) readEntry(reader *bufio.Reader) (Entry, error) {
	var entry Entry

	// Read sequence number (8 bytes)
	if err := binary.Read(reader, binary.LittleEndian, &entry.SeqNum); err != nil {
		return entry, err
	}

	// Read metadata (8 bytes)
	if err := binary.Read(reader, binary.LittleEndian, &entry.Meta); err != nil {
		return entry, err
	}

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
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		maxSeq = max(maxSeq, entry.SeqNum)
	}

	w.seqNum = maxSeq
	return nil
}

func (w *WAL) Done() <-chan seqNum {
	return w.doneCh
}
