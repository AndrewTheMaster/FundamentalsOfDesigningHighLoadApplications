package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"lsmdb/pkg/compression"
)

func main() {
	var (
		mode      = flag.String("mode", "compress", "mode: compress, decompress, benchmark")
		input     = flag.String("input", "", "input file path")
		output    = flag.String("output", "", "output file path (optional)")
		algorithm = flag.String("algo", "lz77", "algorithm: lz77, gzip, zstd")
		skipLZ77  = flag.Bool("skip-lz77", false, "skip LZ77 in benchmark (faster)")
	)
	flag.Parse()

	if *input == "" {
		log.Fatal("input file is required")
	}

	switch *mode {
	case "compress":
		if err := compress(*input, *output, *algorithm); err != nil {
			log.Fatalf("compress failed: %v", err)
		}
	case "decompress":
		if err := decompress(*input, *output, *algorithm); err != nil {
			log.Fatalf("decompress failed: %v", err)
		}
	case "benchmark":
		if err := benchmark(*input, *skipLZ77); err != nil {
			log.Fatalf("benchmark failed: %v", err)
		}
	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}

func compress(inputPath, outputPath, algo string) error {
	if outputPath == "" {
		outputPath = inputPath + ".compressed"
	}

	in, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	start := time.Now()
	var compressed int64

	switch algo {
	case "lz77":
		compressed, err = compression.CompressLZ77(in, out)
	case "gzip":
		compressed, err = compression.CompressGzip(in, out)
	case "zstd":
		compressed, err = compression.CompressZstd(in, out)
	default:
		return fmt.Errorf("unknown algorithm: %s", algo)
	}

	if err != nil {
		return fmt.Errorf("compression failed: %w", err)
	}

	duration := time.Since(start)

	info, _ := os.Stat(inputPath)
	originalSize := info.Size()
	ratio := float64(compressed) / float64(originalSize) * 100

	fmt.Printf("Compression complete:\n")
	fmt.Printf("  Algorithm: %s\n", algo)
	fmt.Printf("  Original: %d bytes\n", originalSize)
	fmt.Printf("  Compressed: %d bytes\n", compressed)
	fmt.Printf("  Ratio: %.2f%%\n", ratio)
	fmt.Printf("  Time: %v\n", duration)

	return nil
}

func decompress(inputPath, outputPath, algo string) error {
	if outputPath == "" {
		outputPath = inputPath + ".decompressed"
	}

	in, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	start := time.Now()
	var decompressed int64

	switch algo {
	case "lz77":
		decompressed, err = compression.DecompressLZ77(in, out)
	case "gzip":
		decompressed, err = compression.DecompressGzip(in, out)
	case "zstd":
		decompressed, err = compression.DecompressZstd(in, out)
	default:
		return fmt.Errorf("unknown algorithm: %s", algo)
	}

	if err != nil {
		return fmt.Errorf("decompression failed: %w", err)
	}

	duration := time.Since(start)

	fmt.Printf("Decompression complete:\n")
	fmt.Printf("  Algorithm: %s\n", algo)
	fmt.Printf("  Decompressed: %d bytes\n", decompressed)
	fmt.Printf("  Time: %v\n", duration)

	return nil
}

func benchmark(inputPath string, skipLZ77 bool) error {
	info, err := os.Stat(inputPath)
	if err != nil {
		return fmt.Errorf("stat input: %w", err)
	}
	originalSize := info.Size()

	algorithms := []string{"lz77", "gzip", "zstd"}
	if skipLZ77 {
		algorithms = []string{"gzip", "zstd"}
	}
	results := make(map[string]compression.BenchResult)

	for _, algo := range algorithms {
		fmt.Printf("\nBenchmarking %s...\n", algo)

		// Compress
		in, err := os.Open(inputPath)
		if err != nil {
			fmt.Printf("  Failed to open input: %v\n", err)
			continue
		}
		compressedPath := inputPath + "." + algo + ".tmp"
		out, err := os.Create(compressedPath)
		if err != nil {
			in.Close()
			fmt.Printf("  Failed to create output: %v\n", err)
			continue
		}

		start := time.Now()
		var compressedSize int64

		switch algo {
		case "lz77":
			compressedSize, err = compression.CompressLZ77(in, out)
		case "gzip":
			compressedSize, err = compression.CompressGzip(in, out)
		case "zstd":
			compressedSize, err = compression.CompressZstd(in, out)
		}

		in.Close()
		out.Close()

		if err != nil {
			fmt.Printf("  Compression failed: %v\n", err)
			os.Remove(compressedPath)
			continue
		}

		compressTime := time.Since(start)

		// Decompress
		in.Close()
		out.Close()

		in, err = os.Open(compressedPath)
		if err != nil {
			fmt.Printf("  Failed to open compressed file: %v\n", err)
			os.Remove(compressedPath)
			continue
		}
		decompressedPath := inputPath + "." + algo + ".decompressed"
		out, err = os.Create(decompressedPath)
		if err != nil {
			in.Close()
			fmt.Printf("  Failed to create decompressed file: %v\n", err)
			os.Remove(compressedPath)
			continue
		}

		start = time.Now()
		var decompressedSize int64

		switch algo {
		case "lz77":
			decompressedSize, err = compression.DecompressLZ77(in, out)
		case "gzip":
			decompressedSize, err = compression.DecompressGzip(in, out)
		case "zstd":
			decompressedSize, err = compression.DecompressZstd(in, out)
		}

		in.Close()
		out.Close()

		if err != nil {
			fmt.Printf("  Decompression failed: %v\n", err)
			os.Remove(compressedPath)
			os.Remove(decompressedPath)
			continue
		}

		decompressTime := time.Since(start)

		in.Close()
		out.Close()

		// Verify
		if decompressedSize != originalSize {
			fmt.Printf("  WARNING: Size mismatch! Original: %d, Decompressed: %d\n",
				originalSize, decompressedSize)
		}

		ratio := float64(compressedSize) / float64(originalSize) * 100
		compressionRatio := float64(originalSize) / float64(compressedSize)

		results[algo] = compression.BenchResult{
			Algorithm:        algo,
			OriginalSize:     originalSize,
			CompressedSize:   compressedSize,
			CompressTime:     compressTime,
			DecompressTime:   decompressTime,
			Ratio:            ratio,
			CompressionRatio: compressionRatio,
		}

		// Cleanup
		os.Remove(compressedPath)
		os.Remove(decompressedPath)
	}

	// Print comparison table
	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("COMPRESSION BENCHMARK RESULTS\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("%-10s %12s %12s %10s %10s %10s %10s\n",
		"Algorithm", "Original", "Compressed", "Ratio %", "Compress", "Decompress", "Ratio")
	fmt.Printf("%-10s %12s %12s %10s %10s %10s %10s\n",
		"---------", "--------", "----------", "-------", "--------", "----------", "-----")

	for _, algo := range algorithms {
		r, ok := results[algo]
		if !ok {
			continue
		}
		fmt.Printf("%-10s %12d %12d %9.2f%% %9v %10v %9.2fx\n",
			r.Algorithm,
			r.OriginalSize,
			r.CompressedSize,
			r.Ratio,
			r.CompressTime,
			r.DecompressTime,
			r.CompressionRatio,
		)
	}

	return nil
}
