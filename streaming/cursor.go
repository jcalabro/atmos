package streaming

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// CursorStore persists firehose cursor positions for crash recovery.
type CursorStore interface {
	// LoadCursor reads the last persisted cursor position. Returns 0 if no cursor exists.
	LoadCursor(ctx context.Context) (int64, error)
	// SaveCursor writes the cursor position to durable storage.
	SaveCursor(ctx context.Context, cursor int64) error
}

// FileCursorStore persists the cursor as a plain int64 in a file.
// Writes use atomic temp+rename for crash safety. Safe for single-process use.
type FileCursorStore struct {
	path string
}

// NewFileCursorStore creates a FileCursorStore that reads/writes the given path.
func NewFileCursorStore(path string) *FileCursorStore {
	return &FileCursorStore{path: path}
}

// LoadCursor reads the cursor from disk. Returns 0 if the file does not exist.
func (s *FileCursorStore) LoadCursor(_ context.Context) (int64, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read cursor file: %w", err)
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse cursor: %w", err)
	}
	return v, nil
}

// SaveCursor writes the cursor to disk atomically via temp+rename.
func (s *FileCursorStore) SaveCursor(_ context.Context, cursor int64) error {
	dir := filepath.Dir(s.path)
	f, err := os.CreateTemp(dir, ".cursor-*")
	if err != nil {
		return fmt.Errorf("create temp cursor file: %w", err)
	}
	tmp := f.Name()
	if _, err := fmt.Fprintf(f, "%d\n", cursor); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write cursor: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close cursor file: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename cursor file: %w", err)
	}
	return nil
}
