package compression

import (
	"compress/gzip"
	"io"
	"time"

	"github.com/klauspost/compress/zstd"
)

// BenchResult holds benchmark results
type BenchResult struct {
	Algorithm        string
	OriginalSize     int64
	CompressedSize   int64
	CompressTime     time.Duration
	DecompressTime   time.Duration
	Ratio            float64
	CompressionRatio float64
}

// CompressGzip compresses using standard gzip
func CompressGzip(r io.Reader, w io.Writer) (int64, error) {
	counter := &byteCounter{w: w}
	gz := gzip.NewWriter(counter)
	defer gz.Close()

	_, err := io.Copy(gz, r)
	if err != nil {
		return 0, err
	}

	if err := gz.Close(); err != nil {
		return 0, err
	}

	return counter.Count(), nil
}

// DecompressGzip decompresses gzip data
func DecompressGzip(r io.Reader, w io.Writer) (int64, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return 0, err
	}
	defer gz.Close()

	return io.Copy(w, gz)
}

// CompressZstd compresses using zstd
func CompressZstd(r io.Reader, w io.Writer) (int64, error) {
	counter := &byteCounter{w: w}
	enc, err := zstd.NewWriter(counter)
	if err != nil {
		return 0, err
	}
	defer enc.Close()

	_, err = io.Copy(enc, r)
	if err != nil {
		return 0, err
	}

	if err := enc.Close(); err != nil {
		return 0, err
	}

	return counter.Count(), nil
}

// DecompressZstd decompresses zstd data
func DecompressZstd(r io.Reader, w io.Writer) (int64, error) {
	dec, err := zstd.NewReader(r)
	if err != nil {
		return 0, err
	}
	defer dec.Close()

	return io.Copy(w, dec)
}
