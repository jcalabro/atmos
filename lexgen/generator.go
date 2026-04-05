// Package lexgen generates Go types and client functions from ATProto Lexicon schemas.
package lexgen

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/jcalabro/atmos/lexicon"
)

// Config controls code generation.
type Config struct {
	Packages          []PackageConfig `json:"packages"`
	SharedTypesDir    string          `json:"sharedTypesDir"`    // output directory for shared types, e.g. "api/lextypes"
	SharedTypesPkg    string          `json:"sharedTypesPkg"`    // Go package name, e.g. "lextypes"
	SharedTypesImport string          `json:"sharedTypesImport"` // full import path, e.g. "github.com/jcalabro/atmos/api/lextypes"
}

// PackageConfig maps an NSID prefix to a Go package.
type PackageConfig struct {
	Prefix  string `json:"prefix"`  // NSID prefix, e.g. "app.bsky"
	Package string `json:"package"` // Go package name, e.g. "bsky"
	OutDir  string `json:"outDir"`  // output directory, e.g. "api/bsky"
	Import  string `json:"import"`  // full import path, e.g. "github.com/jcalabro/atmos/api/bsky"
}

// Generate produces Go source files from parsed lexicon schemas.
// Returns a map of file path → file contents.
func Generate(cfg *Config, cat *lexicon.Catalog) (map[string][]byte, error) {
	type rawFile struct {
		path string
		code string
	}
	var raw []rawFile

	for _, s := range cat.Schemas() {
		pkg := findPackage(cfg, s.ID)
		if pkg == nil {
			continue // schema not in any configured package
		}

		g := &fileGen{
			schema:  s,
			pkg:     pkg,
			cfg:     cfg,
			cat:     cat,
			imports: make(map[string]bool),
		}

		code, err := g.generate()
		if err != nil {
			return nil, fmt.Errorf("lexgen: %s: %w", s.ID, err)
		}
		if code == "" {
			continue // no output (e.g. all tokens, no structs)
		}

		fname := schemaFileName(s.ID)
		path := pkg.OutDir + "/" + fname
		raw = append(raw, rawFile{path: path, code: code})
	}

	// Generate shared types into the dedicated shared package.
	if cfg.SharedTypesDir != "" {
		code := generateSharedTypes(cfg.SharedTypesPkg)
		path := cfg.SharedTypesDir + "/types.go"
		raw = append(raw, rawFile{path: path, code: code})
	} else {
		// Fallback: emit into each package (legacy behavior).
		for _, pkg := range cfg.Packages {
			code := generateSharedTypes(pkg.Package)
			path := pkg.OutDir + "/types.go"
			raw = append(raw, rawFile{path: path, code: code})
		}
	}

	// Generate extra field types and helpers for each package.
	// Each package gets its own copy so the types stay unexported.
	for _, pkg := range cfg.Packages {
		code := generateExtraTypes(pkg.Package)
		path := pkg.OutDir + "/extra.go"
		raw = append(raw, rawFile{path: path, code: code})
	}
	if cfg.SharedTypesDir != "" {
		code := generateExtraTypes(cfg.SharedTypesPkg)
		path := cfg.SharedTypesDir + "/extra.go"
		raw = append(raw, rawFile{path: path, code: code})
	}

	// Generate DecodeRecord function for each package that has record types.
	for _, pkg := range cfg.Packages {
		// Collect all record types in this package.
		var records []recordInfo
		for _, s := range cat.Schemas() {
			p := findPackage(cfg, s.ID)
			if p == nil || p.Package != pkg.Package {
				continue
			}
			if def, ok := s.Defs["main"]; ok && def.Type == "record" {
				records = append(records, recordInfo{
					nsid:     s.ID,
					typeName: TypeName(s.ID, "main"),
				})
			}
		}
		if len(records) == 0 {
			continue
		}

		// Sort for determinism.
		sort.Slice(records, func(i, j int) bool {
			return records[i].nsid < records[j].nsid
		})

		code := generateDecodeRecord(pkg.Package, records)
		path := pkg.OutDir + "/decode.go"
		raw = append(raw, rawFile{path: path, code: code})
	}

	// Format all files in parallel.
	type fmtResult struct {
		path string
		data []byte
		err  error
	}
	results := make([]fmtResult, len(raw))
	var wg sync.WaitGroup
	for i, f := range raw {
		wg.Go(func() {
			formatted, err := formatSource(f.path, []byte(f.code))
			results[i] = fmtResult{path: f.path, data: formatted, err: err}
		})
	}
	wg.Wait()

	files := make(map[string][]byte, len(results))
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		files[r.path] = r.data
	}
	return files, nil
}

// findPackage returns the PackageConfig for the given NSID, or nil.
func findPackage(cfg *Config, nsid string) *PackageConfig {
	var best *PackageConfig
	bestLen := 0
	for i := range cfg.Packages {
		p := &cfg.Packages[i]
		if strings.HasPrefix(nsid, p.Prefix) && len(p.Prefix) > bestLen {
			best = p
			bestLen = len(p.Prefix)
		}
	}
	return best
}

// schemaFileName returns the output file name for a schema.
// e.g. "app.bsky.feed.post" → "feedpost.go"
func schemaFileName(nsid string) string {
	parts := strings.Split(nsid, ".")
	if len(parts) < 2 {
		return strings.ToLower(nsid) + ".go"
	}
	// Take last two segments.
	name := strings.ToLower(parts[len(parts)-2] + parts[len(parts)-1])
	return name + ".go"
}

// fileGen generates code for a single schema file.
type fileGen struct {
	schema  *lexicon.Schema
	pkg     *PackageConfig
	cfg     *Config
	cat     *lexicon.Catalog
	imports map[string]bool
}

// sharedType returns the qualified name for a shared type (LexBlob, LexCIDLink,
// UnknownUnionVariant) and adds the import if a shared types package is configured.
func (g *fileGen) sharedType(name string) string {
	if g.cfg.SharedTypesImport != "" {
		alias := g.cfg.SharedTypesPkg
		g.imports[alias+" \""+g.cfg.SharedTypesImport+"\""] = true
		return alias + "." + name
	}
	return name
}

func (g *fileGen) generate() (string, error) {
	var sections []string

	// Collect all defs, sorted by name for determinism.
	defNames := sortedDefNames(g.schema)

	// Generate constants: tokens and NSID constants for record types.
	var consts []string
	for _, name := range defNames {
		def := g.schema.Defs[name]
		if def.Type == "token" {
			consts = append(consts, g.genToken(name, def))
		}
		if def.Type == "record" && name == "main" {
			consts = append(consts, g.genNSID(name, def))
		}
	}
	if len(consts) > 0 {
		sections = append(sections, "const (\n"+strings.Join(consts, "")+"\n)")
	}

	// Generate types and functions.
	for _, name := range defNames {
		def := g.schema.Defs[name]
		code, err := g.genDef(name, def)
		if err != nil {
			return "", err
		}
		if code != "" {
			sections = append(sections, code)
		}
	}

	if len(sections) == 0 {
		return "", nil
	}

	// Build complete file.
	var out strings.Builder
	out.WriteString("// Code generated by lexgen. DO NOT EDIT.\n\n")
	out.WriteString("package " + g.pkg.Package + "\n\n")

	// Emit explicit import block so goimports doesn't guess wrong.
	if len(g.imports) > 0 {
		importLines := make([]string, 0, len(g.imports))
		for imp := range g.imports {
			// Aliased imports already have quotes (e.g. `bsky "path"`).
			// Non-aliased imports need to be quoted.
			if strings.Contains(imp, "\"") {
				importLines = append(importLines, imp)
			} else {
				importLines = append(importLines, "\""+imp+"\"")
			}
		}
		sort.Strings(importLines)
		out.WriteString("import (\n")
		for _, imp := range importLines {
			out.WriteString("\t" + imp + "\n")
		}
		out.WriteString(")\n\n")
	}

	out.WriteString(strings.Join(sections, "\n\n"))
	out.WriteString("\n")

	return out.String(), nil
}

func (g *fileGen) genDef(name string, def *lexicon.Def) (string, error) {
	switch def.Type {
	case "record":
		return g.genRecord(name, def)
	case "query":
		return g.genQuery(name, def)
	case "procedure":
		return g.genProcedure(name, def)
	case "subscription":
		return g.genSubscription(name, def)
	case "object":
		return g.genObject(name, def)
	case "string":
		return g.genStringDef(name, def)
	case "token":
		return "", nil // handled above as consts
	case "union":
		return g.genUnionDef(name, def)
	case "array":
		return g.genArrayDef(name, def)
	case "ref":
		return "", nil // refs don't generate types
	default:
		return "", nil
	}
}

// typeName returns the Go type name for a def in this schema.
func (g *fileGen) typeName(defName string) string {
	return TypeName(g.schema.ID, defName)
}

// TypeName returns the Go type name for a def given an NSID and def name.
// "app.bsky.feed.post", "main"     → "FeedPost"
// "app.bsky.feed.post", "replyRef" → "FeedPost_ReplyRef"
// "app.bsky.feed.defs", "postView" → "FeedDefs_PostView"
func TypeName(nsid, defName string) string {
	parts := strings.Split(nsid, ".")
	if len(parts) < 2 {
		if defName == "main" {
			return capitalize(nsid)
		}
		return capitalize(nsid) + "_" + capitalize(defName)
	}
	base := capitalize(parts[len(parts)-2]) + capitalize(parts[len(parts)-1])
	if defName == "main" {
		return base
	}
	return base + "_" + capitalize(defName)
}

func capitalize(s string) string {
	if s == "" {
		return ""
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func sortedDefNames(s *lexicon.Schema) []string {
	names := make([]string, 0, len(s.Defs))
	for name := range s.Defs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
