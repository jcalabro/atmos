package lexicon

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ParseFile reads and parses a single lexicon JSON file.
func ParseFile(path string) (*Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("lexicon: read %s: %w", path, err)
	}
	return Parse(data)
}

// Parse parses a lexicon JSON document.
func Parse(data []byte) (*Schema, error) {
	var s Schema
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("lexicon: parse: %w", err)
	}
	if s.Lexicon != 1 {
		return nil, fmt.Errorf("lexicon: unsupported version %d", s.Lexicon)
	}
	if s.ID == "" {
		return nil, fmt.Errorf("lexicon: missing id")
	}
	if s.Defs == nil {
		return nil, fmt.Errorf("lexicon: %s: missing defs", s.ID)
	}
	return &s, nil
}

// ParseDir recursively finds and parses all .json lexicon files under dir.
func ParseDir(dir string) ([]*Schema, error) {
	var schemas []*Schema
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		s, err := ParseFile(path)
		if err != nil {
			return err
		}
		schemas = append(schemas, s)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return schemas, nil
}
