// Command lexgen generates Go types and client functions from ATProto Lexicon JSON schemas.
//
// Usage:
//
//	lexgen -lexdir <dir> -outdir <dir> -config <file>
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jcalabro/atmos/lexgen"
	"github.com/jcalabro/atmos/lexicon"
)

func main() {
	var lexDirs stringListFlag
	flag.Var(&lexDirs, "lexdir", "directory containing lexicon JSON files (repeatable; at least one required)")
	configFile := flag.String("config", "", "config JSON file (required)")
	outputRoot := flag.String("output-root", "", "root directory for generated files (optional)")
	flag.Parse()

	if len(lexDirs) == 0 || *configFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	if err := run(lexDirs, *configFile, *outputRoot); err != nil {
		fmt.Fprintf(os.Stderr, "lexgen: %v\n", err)
		os.Exit(1)
	}
}

type stringListFlag []string

func (f *stringListFlag) String() string { return strings.Join(*f, ",") }

func (f *stringListFlag) Set(value string) error {
	if value == "" {
		return fmt.Errorf("lexicon directory must not be empty")
	}
	*f = append(*f, value)
	return nil
}

func run(lexDirs []string, configFile, outputRoot string) error {
	// Load config.
	cfgData, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	var cfg lexgen.Config
	if err := json.Unmarshal(cfgData, &cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if outputRoot != "" {
		if err := rebaseOutputDirs(&cfg, outputRoot); err != nil {
			return err
		}
	}

	// Parse all roots into one catalog. A single catalog is required because
	// package-wide generated helpers must see records from every input root.
	cat := lexicon.NewCatalog()
	parsed := 0
	for _, lexDir := range lexDirs {
		schemas, err := lexicon.ParseDir(lexDir)
		if err != nil {
			return fmt.Errorf("parse lexicons from %s: %w", lexDir, err)
		}
		if err := cat.AddAll(schemas); err != nil {
			return fmt.Errorf("catalog root %s: %w", lexDir, err)
		}
		parsed += len(schemas)
	}
	fmt.Fprintf(os.Stderr, "parsed %d lexicon schemas from %d roots\n", parsed, len(lexDirs))

	if err := cat.Resolve(); err != nil {
		return err
	}

	// Generate code.
	files, err := lexgen.Generate(&cfg, cat)
	if err != nil {
		return err
	}

	// Write files.
	if err := lexgen.WriteFiles(files); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %d files\n", len(files))
	return nil
}

func rebaseOutputDirs(cfg *lexgen.Config, root string) error {
	for i := range cfg.Packages {
		outDir, err := outputPath(root, cfg.Packages[i].OutDir)
		if err != nil {
			return fmt.Errorf("package %s output directory: %w", cfg.Packages[i].Prefix, err)
		}
		cfg.Packages[i].OutDir = outDir
	}
	if cfg.SharedTypesDir == "" {
		return nil
	}
	sharedTypesDir, err := outputPath(root, cfg.SharedTypesDir)
	if err != nil {
		return fmt.Errorf("shared types output directory: %w", err)
	}
	cfg.SharedTypesDir = sharedTypesDir
	return nil
}

func outputPath(root, path string) (string, error) {
	cleaned := filepath.Clean(path)
	if filepath.IsAbs(path) || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("must be a relative path within the output root: %q", path)
	}
	return filepath.Join(root, cleaned), nil
}
