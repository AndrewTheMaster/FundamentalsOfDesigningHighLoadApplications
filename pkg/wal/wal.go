package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"lsmdb/pkg/listener"
	"lsmdb/pkg/types"
	"math"
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
	*listener.Listener[Entry]

	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	filePath string

	inputCh chan Entry
	doneCh  chan seqNum
}

// New creates a new WAL instance
func New(dir string) (*WAL, error) {
	// Ensure directory is a clean path and create with restrictive permissions
	if dir == "" {
		return nil, fmt.Errorf("empty WAL dir")
	}
	dir = filepath.Clean(dir)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	filePath := filepath.Join(dir, "wal.log")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	wal := &WAL{
		file:     file,
		writer:   bufio.NewWriter(file),
		filePath: filePath,
		inputCh:  make(chan Entry, 3),
		doneCh:   make(chan seqNum, 3),
	}

	// Initialize channels and listener write listener
	wal.Listener = listener.New(wal.inputCh, wal.writeFile, wal.stop)

	return wal, nil
}

func (w *WAL) Append(entry Entry) {
	w.inputCh <- entry
}

// will be called async by WAL.listener on input in WAL.inputCh
func (w *WAL) writeFile(entry Entry) error {
	if err := w.writeEntry(entry); err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
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
	defer func() {
		if cerr := file.Close(); cerr != nil {
			slog.Warn("failed to close WAL read file", "error", cerr)
		}
	}()

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

	// Write key length (4 bytes) - ensure it fits into uint32
	if len(entry.Key) > math.MaxUint32 {
		return fmt.Errorf("key too large: %d", len(entry.Key))
	}
	keyLen := uint32(len(entry.Key))
	if err := binary.Write(w.writer, binary.LittleEndian, keyLen); err != nil {
		return err
	}

	// Write key
	if _, err := w.writer.Write(entry.Key); err != nil {
		return err
	}

	// Write value length (4 bytes) - ensure it fits into uint32
	if len(entry.Value) > math.MaxUint32 {
		return fmt.Errorf("value too large: %d", len(entry.Value))
	}
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

func (w *WAL) Done() <-chan seqNum {
	return w.doneCh
}

func (w *WAL) stop() {
	close(w.inputCh)
	close(w.doneCh)
}
