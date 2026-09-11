// Package declaration validates and resolves AT Protocol space-type declarations.
package declaration

import (
	"fmt"
	"strings"
	"unicode/utf16"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/lexicon"
)

// Declaration is a validated space-type declaration.
type Declaration struct {
	Type        atmos.NSID
	Name        string
	Names       map[atmos.Language]string
	Description string
	Key         string
	Collections []atmos.NSID
}

// Validate validates the main definition of schema as a space declaration.
func Validate(schema *lexicon.Schema) (*Declaration, error) {
	if schema == nil {
		return nil, fmt.Errorf("space declaration: nil schema")
	}
	if schema.Lexicon != 1 {
		return nil, fmt.Errorf("space declaration: unsupported lexicon version %d", schema.Lexicon)
	}
	typeID, err := atmos.ParseNSID(schema.ID)
	if err != nil {
		return nil, fmt.Errorf("space declaration: invalid schema id: %w", err)
	}
	main, ok := schema.Defs["main"]
	if !ok || main == nil {
		return nil, fmt.Errorf("space declaration %s: missing defs.main", typeID)
	}
	if main.Type != "space" {
		return nil, fmt.Errorf("space declaration %s: defs.main type is %q, want space", typeID, main.Type)
	}
	if err := validateName(main.Name); err != nil {
		return nil, fmt.Errorf("space declaration %s name: %w", typeID, err)
	}
	if err := validateKey(main.Key); err != nil {
		return nil, fmt.Errorf("space declaration %s key: %w", typeID, err)
	}
	if main.Collections == nil {
		return nil, fmt.Errorf("space declaration %s: missing collections", typeID)
	}

	result := &Declaration{
		Type:        typeID,
		Name:        main.Name,
		Names:       make(map[atmos.Language]string, len(main.Names)),
		Description: main.Desc,
		Key:         main.Key,
		Collections: make([]atmos.NSID, 0, len(main.Collections)),
	}
	seenLanguages := make(map[string]struct{}, len(main.Names))
	for rawLanguage, name := range main.Names {
		language, err := atmos.ParseLanguage(rawLanguage)
		if err != nil {
			return nil, fmt.Errorf("space declaration %s localized name language %q: %w", typeID, rawLanguage, err)
		}
		if err := validateName(name); err != nil {
			return nil, fmt.Errorf("space declaration %s localized name %q: %w", typeID, rawLanguage, err)
		}
		canonical := strings.ToLower(rawLanguage)
		if _, exists := seenLanguages[canonical]; exists {
			return nil, fmt.Errorf("space declaration %s: duplicate localized language %q", typeID, rawLanguage)
		}
		seenLanguages[canonical] = struct{}{}
		result.Names[language] = name
	}
	seen := make(map[atmos.NSID]struct{}, len(main.Collections))
	for i, rawCollection := range main.Collections {
		collection, err := atmos.ParseNSID(rawCollection)
		if err != nil {
			return nil, fmt.Errorf("space declaration %s collections[%d]: %w", typeID, i, err)
		}
		if _, exists := seen[collection]; exists {
			return nil, fmt.Errorf("space declaration %s collections[%d]: duplicate %q", typeID, i, collection)
		}
		seen[collection] = struct{}{}
		result.Collections = append(result.Collections, collection)
	}
	return result, nil
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("must not be empty")
	}
	if len(utf16.Encode([]rune(name))) > 64 {
		return fmt.Errorf("exceeds 64 UTF-16 code units")
	}
	return nil
}

func validateKey(key string) error {
	switch key {
	case "any", "nsid", "tid":
		return nil
	}
	literal, ok := strings.CutPrefix(key, "literal:")
	if !ok || literal == "" {
		return fmt.Errorf("must be any, nsid, tid, or literal:<record-key>")
	}
	if _, err := atmos.ParseRecordKey(literal); err != nil {
		return fmt.Errorf("invalid literal: %w", err)
	}
	return nil
}

func cloneDeclaration(in *Declaration) *Declaration {
	if in == nil {
		return nil
	}
	out := *in
	out.Names = make(map[atmos.Language]string, len(in.Names))
	for language, name := range in.Names {
		out.Names[language] = name
	}
	out.Collections = append([]atmos.NSID(nil), in.Collections...)
	return &out
}
