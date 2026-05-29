package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	var outputPath string
	var sizeBytes int64

	flag.StringVar(&outputPath, "out", "artifacts/sample.bin", "output file path")
	flag.Int64Var(&sizeBytes, "size", 1048576, "file size in bytes")
	flag.Parse()

	if sizeBytes < 0 {
		fmt.Fprintln(os.Stderr, "size must be non-negative")
		os.Exit(1)
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer file.Close()

	pattern := []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	buf := make([]byte, 32*1024)
	for i := range buf {
		buf[i] = pattern[i%len(pattern)]
	}

	remaining := sizeBytes
	for remaining > 0 {
		chunk := int64(len(buf))
		if remaining < chunk {
			chunk = remaining
		}
		if _, err := file.Write(buf[:chunk]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		remaining -= chunk
	}

	if err := file.Sync(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("generated %s (%d bytes)\n", outputPath, sizeBytes)
}
