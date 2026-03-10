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

	"github.com/jcalabro/atmos/lexgen"
	"github.com/jcalabro/atmos/lexicon"
)

func main() {
	lexDir := flag.String("lexdir", "", "directory containing lexicon JSON files (required)")
	configFile := flag.String("config", "", "config JSON file (required)")
	flag.Parse()

	if *lexDir == "" || *configFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	if err := run(*lexDir, *configFile); err != nil {
		fmt.Fprintf(os.Stderr, "lexgen: %v\n", err)
		os.Exit(1)
	}
}

func run(lexDir, configFile string) error {
	// Load config.
	cfgData, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	var cfg lexgen.Config
	if err := json.Unmarshal(cfgData, &cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	// Parse lexicons.
	schemas, err := lexicon.ParseDir(lexDir)
	if err != nil {
		return fmt.Errorf("parse lexicons: %w", err)
	}
	fmt.Fprintf(os.Stderr, "parsed %d lexicon schemas\n", len(schemas))

	// Build catalog and resolve refs.
	cat := lexicon.NewCatalog()
	if err := cat.AddAll(schemas); err != nil {
		return err
	}
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
