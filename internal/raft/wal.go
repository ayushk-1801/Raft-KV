package raft

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

// WAL handles append-only log persistence with fsync support.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, path: path}, nil
}

func (w *WAL) AppendLog(entry []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var sz [4]byte
	binary.BigEndian.PutUint32(sz[:], uint32(len(entry)))
	if _, err := w.f.Write(sz[:]); err != nil {
		return err
	}
	if _, err := w.f.Write(entry); err != nil {
		return err
	}
	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

func (w *WAL) ReadLogRange(start, end int) ([][]byte, error) {
	entries, err := w.Load()
	if err != nil {
		return nil, err
	}
	if start < 0 {
		start = 0
	}
	if end > len(entries) {
		end = len(entries)
	}
	if start > end {
		return nil, nil
	}
	return entries[start:end], nil
}

func (w *WAL) Load() ([][]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, 0); err != nil {
		return nil, err
	}
	var entries [][]byte
	for {
		var sz [4]byte
		if _, err := io.ReadFull(w.f, sz[:]); err != nil {
			if err == io.EOF {
				break
			}
			if err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		length := binary.BigEndian.Uint32(sz[:])
		data := make([]byte, length)
		if _, err := io.ReadFull(w.f, data); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		entries = append(entries, data)
	}
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return entries, nil
}

func (w *WAL) ClearLog() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, 0); err != nil {
		return err
	}
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
