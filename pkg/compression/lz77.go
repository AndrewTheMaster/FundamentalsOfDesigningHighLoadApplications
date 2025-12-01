package compression

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	// LZ77 parameters
	windowSize  = 32768 // 32KB lookback window
	minMatchLen = 3     // minimum match length to encode
	maxMatchLen = 258   // maximum match length
	maxOffset   = 32768 // maximum offset
)

// LZ77Token represents a single token in LZ77 encoding
type LZ77Token struct {
	Offset    uint16 // distance back in window
	Length    uint8  // length of match
	NextByte  uint8  // next literal byte (0 if not used)
	IsLiteral bool   // true if this is a literal, false if a match
}

// CompressLZ77 compresses data using LZ77 algorithm
func CompressLZ77(r io.Reader, w io.Writer) (int64, error) {
	// Read all input
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, fmt.Errorf("read input: %w", err)
	}

	if len(data) == 0 {
		return 0, nil
	}

	var tokens []LZ77Token
	window := make([]byte, 0, windowSize)
	pos := 0
	lastProgress := 0

	for pos < len(data) {
		// Progress indicator for large files
		if len(data) > 1000000 && pos*100/len(data) > lastProgress+5 {
			lastProgress = pos * 100 / len(data)
			fmt.Fprintf(os.Stderr, "\r[LZ77] Progress: %d%%", lastProgress)
		}
		bestMatch := findBestMatch(data, pos, window)

		if bestMatch.length >= minMatchLen {
			// Encode as match
			tokens = append(tokens, LZ77Token{
				Offset:    uint16(bestMatch.offset),
				Length:    uint8(bestMatch.length),
				IsLiteral: false,
			})

			// Update window
			for i := 0; i < bestMatch.length && pos < len(data); i++ {
				window = append(window, data[pos])
				if len(window) > windowSize {
					window = window[1:]
				}
				pos++
			}
		} else {
			// Encode as literal
			tokens = append(tokens, LZ77Token{
				NextByte:  data[pos],
				IsLiteral: true,
			})

			// Update window
			window = append(window, data[pos])
			if len(window) > windowSize {
				window = window[1:]
			}
			pos++
		}
	}

	if len(data) > 1000000 {
		fmt.Fprintf(os.Stderr, "\r[LZ77] Progress: 100%%\n")
	}

	// Write tokens
	return writeTokens(w, tokens)
}

type match struct {
	offset int
	length int
}

func findBestMatch(data []byte, pos int, window []byte) match {
	if len(window) == 0 || pos >= len(data) {
		return match{offset: 0, length: 0}
	}

	bestLen := 0
	bestOffset := 0
	searchLen := len(window)
	if searchLen > maxOffset {
		searchLen = maxOffset
	}

	// Optimized: start from closest positions first (better compression)
	// Also limit search to reasonable range to avoid O(n*m) complexity
	maxSearch := searchLen
	if maxSearch > 4096 {
		maxSearch = 4096 // Limit search window for performance
	}

	// Search backwards through window, starting from closest
	for offset := 1; offset <= maxSearch && offset <= pos; offset++ {
		windowPos := len(window) - offset
		if windowPos < 0 {
			break
		}

		// Quick check: if first bytes don't match, skip
		if windowPos >= len(window) || data[pos] != window[windowPos] {
			continue
		}

		// Try to match as much as possible
		matchLen := 1
		maxCheck := maxMatchLen
		if maxCheck > len(data)-pos {
			maxCheck = len(data) - pos
		}
		if maxCheck > len(window)-windowPos {
			maxCheck = len(window) - windowPos
		}

		for i := 1; i < maxCheck; i++ {
			if data[pos+i] == window[windowPos+i] {
				matchLen++
			} else {
				break
			}
		}

		if matchLen > bestLen {
			bestLen = matchLen
			bestOffset = offset
			// Early exit if we found a good match
			if bestLen >= maxMatchLen-10 {
				break
			}
		}
	}

	return match{offset: bestOffset, length: bestLen}
}

func writeTokens(w io.Writer, tokens []LZ77Token) (int64, error) {
	var written int64

	// Write number of tokens
	if err := binary.Write(w, binary.LittleEndian, uint32(len(tokens))); err != nil {
		return written, err
	}
	written += 4

	// Write tokens
	for _, token := range tokens {
		if token.IsLiteral {
			// Literal: write 1 byte flag (0xFF) + 1 byte value
			if _, err := w.Write([]byte{0xFF, token.NextByte}); err != nil {
				return written, err
			}
			written += 2
		} else {
			// Match: write 2 bytes offset + 1 byte length
			// Use 0xFE as flag for match (to distinguish from literal)
			if err := binary.Write(w, binary.LittleEndian, uint8(0xFE)); err != nil {
				return written, err
			}
			written++
			if err := binary.Write(w, binary.LittleEndian, token.Offset); err != nil {
				return written, err
			}
			written += 2
			if err := binary.Write(w, binary.LittleEndian, token.Length); err != nil {
				return written, err
			}
			written++
		}
	}

	return written, nil
}

// DecompressLZ77 decompresses LZ77-compressed data
func DecompressLZ77(r io.Reader, w io.Writer) (int64, error) {
	// Read number of tokens
	var tokenCount uint32
	if err := binary.Read(r, binary.LittleEndian, &tokenCount); err != nil {
		return 0, fmt.Errorf("read token count: %w", err)
	}

	window := make([]byte, 0, windowSize)
	var written int64

	for i := uint32(0); i < tokenCount; i++ {
		flag := make([]byte, 1)
		if _, err := io.ReadFull(r, flag); err != nil {
			return written, fmt.Errorf("read flag: %w", err)
		}

		if flag[0] == 0xFF {
			// Literal
			literal := make([]byte, 1)
			if _, err := io.ReadFull(r, literal); err != nil {
				return written, fmt.Errorf("read literal: %w", err)
			}

			if _, err := w.Write(literal); err != nil {
				return written, err
			}
			written++

			// Update window
			window = append(window, literal[0])
			if len(window) > windowSize {
				window = window[1:]
			}
		} else if flag[0] == 0xFE {
			// Match
			var offset uint16
			if err := binary.Read(r, binary.LittleEndian, &offset); err != nil {
				return written, fmt.Errorf("read offset: %w", err)
			}

			var length uint8
			if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
				return written, fmt.Errorf("read length: %w", err)
			}

			// Copy from window
			windowPos := len(window) - int(offset)
			if windowPos < 0 || windowPos >= len(window) {
				return written, fmt.Errorf("invalid offset: %d (window size: %d)", offset, len(window))
			}

			for j := uint8(0); j < length; j++ {
				if windowPos+int(j) >= len(window) {
					return written, fmt.Errorf("match out of bounds")
				}
				b := window[windowPos+int(j)]

				if _, err := w.Write([]byte{b}); err != nil {
					return written, err
				}
				written++

				// Update window
				window = append(window, b)
				if len(window) > windowSize {
					window = window[1:]
				}
			}
		} else {
			return written, fmt.Errorf("unknown flag: 0x%02x", flag[0])
		}
	}

	return written, nil
}
