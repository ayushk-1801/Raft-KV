package raft

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestWALLoadIgnoresTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	writeEntry := func(data []byte) {
		var sz [4]byte
		binary.BigEndian.PutUint32(sz[:], uint32(len(data)))
		if _, err := f.Write(sz[:]); err != nil {
			t.Fatalf("write size: %v", err)
		}
		if _, err := f.Write(data); err != nil {
			t.Fatalf("write data: %v", err)
		}
	}

	writeEntry([]byte("ok"))
	var truncatedSize [4]byte
	binary.BigEndian.PutUint32(truncatedSize[:], 5)
	if _, err := f.Write(truncatedSize[:]); err != nil {
		t.Fatalf("write truncated size: %v", err)
	}
	if _, err := f.Write([]byte("xy")); err != nil {
		t.Fatalf("write truncated data: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close wal seed file: %v", err)
	}

	wal, err := NewWAL(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer wal.Close()

	entries, err := wal.Load()
	if err != nil {
		t.Fatalf("load wal: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one intact entry, got %d", len(entries))
	}
	if string(entries[0]) != "ok" {
		t.Fatalf("expected intact entry %q, got %q", "ok", string(entries[0]))
	}
}
