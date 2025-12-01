package compression

import "io"

// byteCounter wraps an io.Writer and counts bytes written
type byteCounter struct {
	w     io.Writer
	count int64
}

func (bc *byteCounter) Write(p []byte) (int, error) {
	n, err := bc.w.Write(p)
	bc.count += int64(n)
	return n, err
}

func (bc *byteCounter) Count() int64 {
	return bc.count
}
