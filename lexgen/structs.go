package lexgen

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jcalabro/atmos/lexicon"
)

// genRecord generates a record type with its object struct.
func (g *fileGen) genRecord(defName string, def *lexicon.Def) (string, error) {
	if def.Record == nil {
		return "", nil
	}
	typeName := g.typeName(defName)

	var buf strings.Builder
	comment := fmt.Sprintf("// %s is a %q record.", typeName, g.schema.ID)
	if def.Desc != "" {
		comment = fmt.Sprintf("// %s is a %q record.\n//\n// %s", typeName, g.schema.ID, def.Desc)
	}
	buf.WriteString(comment + "\n")

	structCode, extras, err := g.genStructBody(typeName, def.Record, true)
	if err != nil {
		return "", err
	}
	buf.WriteString(structCode)
	for _, extra := range extras {
		buf.WriteString("\n\n" + extra)
	}

	// Generate CBOR marshal/unmarshal for records.
	cborCode, err := g.genCBORMethods(typeName, def.Record, true)
	if err != nil {
		return "", err
	}
	buf.WriteString("\n\n" + cborCode)

	// Generate JSON marshal/unmarshal for records.
	jsonCode, err := g.genJSONMethods(typeName, def.Record, true)
	if err != nil {
		return "", err
	}
	buf.WriteString("\n\n" + jsonCode)

	return buf.String(), nil
}

// genObject generates a standalone object type.
func (g *fileGen) genObject(defName string, def *lexicon.Def) (string, error) {
	typeName := g.typeName(defName)
	obj := &lexicon.Object{
		Properties: def.Properties,
		Required:   def.Required,
		Nullable:   def.Nullable,
	}

	var buf strings.Builder
	comment := fmt.Sprintf("// %s is a %q in the %s schema.", typeName, defName, g.schema.ID)
	if def.Desc != "" {
		comment = fmt.Sprintf("// %s is a %q in the %s schema.\n//\n// %s", typeName, defName, g.schema.ID, def.Desc)
	}
	buf.WriteString(comment + "\n")

	structCode, extras, err := g.genStructBody(typeName, obj, false)
	if err != nil {
		return "", err
	}
	buf.WriteString(structCode)
	for _, extra := range extras {
		buf.WriteString("\n\n" + extra)
	}

	// Generate CBOR marshal/unmarshal for objects.
	cborCode, err := g.genCBORMethods(typeName, obj, false)
	if err != nil {
		return "", err
	}
	buf.WriteString("\n\n" + cborCode)

	// Generate JSON marshal/unmarshal for objects.
	jsonCode, err := g.genJSONMethods(typeName, obj, false)
	if err != nil {
		return "", err
	}
	buf.WriteString("\n\n" + jsonCode)

	return buf.String(), nil
}

// genStructBody generates the struct type definition and any nested types.
// isRecord adds the LexiconTypeID field.
func (g *fileGen) genStructBody(typeName string, obj *lexicon.Object, isRecord bool) (string, []string, error) {
	requiredSet := makeSet(obj.Required)
	nullableSet := makeSet(obj.Nullable)

	// Sort fields for deterministic output.
	fieldNames := sortedKeys(obj.Properties)

	var buf strings.Builder
	buf.WriteString("type " + typeName + " struct {\n")

	if isRecord {
		buf.WriteString("\tLexiconTypeID string `json:\"$type,omitempty\" cborgen:\"$type,const=" + g.schema.ID + "\"`\n")
	} else {
		// All objects get $type so they can participate in unions.
		buf.WriteString("\tLexiconTypeID string `json:\"$type,omitempty\"`\n")
	}

	var extras []string

	for _, fname := range fieldNames {
		f := obj.Properties[fname]
		required := requiredSet[fname]
		nullable := nullableSet[fname]

		goTyp, err := g.goType(f, typeName, fname, required, nullable)
		if err != nil {
			return "", nil, fmt.Errorf("field %s.%s: %w", typeName, fname, err)
		}

		jsonTag := fname
		if !required {
			if strings.HasPrefix(goTyp, "gt.Option[") {
				jsonTag += ",omitzero"
			} else {
				jsonTag += ",omitempty"
			}
		}

		goFieldName := exportFieldName(fname)
		fmt.Fprintf(&buf, "\t%s %s `json:\"%s\"`", goFieldName, goTyp, jsonTag)
		if f.Desc != "" {
			buf.WriteString(" // " + singleLineDesc(f.Desc))
		}
		buf.WriteString("\n")

		// Generate nested types for inline objects and unions.
		if f.Type == "object" && f.Properties != nil {
			nestedTypeName := typeName + "_" + capitalize(fname)
			nestedObj := &lexicon.Object{
				Properties: f.Properties,
				Required:   f.Required,
				Nullable:   f.Nullable,
			}
			nestedCode, nestedExtras, err := g.genStructBody(nestedTypeName, nestedObj, false)
			if err != nil {
				return "", nil, err
			}
			extras = append(extras, nestedCode)
			extras = append(extras, nestedExtras...)

			// Generate CBOR for nested objects.
			cborCode, err := g.genCBORMethods(nestedTypeName, nestedObj, false)
			if err != nil {
				return "", nil, err
			}
			extras = append(extras, cborCode)

			// Generate JSON for nested objects.
			jsonCode, err := g.genJSONMethods(nestedTypeName, nestedObj, false)
			if err != nil {
				return "", nil, err
			}
			extras = append(extras, jsonCode)
		}

		if f.Type == "union" {
			unionTypeName := typeName + "_" + capitalize(fname)
			unionCode, err := g.genUnionType(unionTypeName, f.Refs, f.Closed)
			if err != nil {
				return "", nil, err
			}
			extras = append(extras, unionCode)
		}

		// Array of unions: generate the union type for the items.
		if f.Type == "array" && f.Items != nil && f.Items.Type == "union" {
			unionTypeName := typeName + "_" + capitalize(fname)
			unionCode, err := g.genUnionType(unionTypeName, f.Items.Refs, f.Items.Closed)
			if err != nil {
				return "", nil, err
			}
			extras = append(extras, unionCode)
		}
	}

	// Extra field preserves unknown JSON/CBOR keys across same-format round-trips.
	// A single slice is used with an encoding tag per entry — see extraField.
	fmt.Fprintf(&buf, "\n\t// extra preserves unknown fields for same-format round-trips.\n")
	fmt.Fprintf(&buf, "\textra []extraField\n")

	buf.WriteString("}")
	return buf.String(), extras, nil
}

// genStringDef generates a type alias and constants for a string definition.
func (g *fileGen) genStringDef(defName string, def *lexicon.Def) (string, error) {
	typeName := g.typeName(defName)
	var buf strings.Builder

	// Always generate a type alias so refs can point to it.
	fmt.Fprintf(&buf, "// %s is a string type.\n", typeName)
	fmt.Fprintf(&buf, "type %s = string\n", typeName)

	values := def.Enum
	if len(values) == 0 {
		values = def.KnownValues
	}
	if len(values) > 0 {
		fmt.Fprintf(&buf, "\n// %s known values.\n", typeName)
		buf.WriteString("const (\n")
		for _, v := range values {
			constName := typeName + "_" + sanitizeConstName(v)
			fmt.Fprintf(&buf, "\t%s = %q\n", constName, v)
		}
		buf.WriteString(")")
	}
	return buf.String(), nil
}

// genArrayDef generates a type alias for a top-level array definition.
func (g *fileGen) genArrayDef(defName string, def *lexicon.Def) (string, error) {
	typeName := g.typeName(defName)

	if def.Items == nil {
		g.imports["encoding/json"] = true
		return fmt.Sprintf("// %s is an array type defined in %s.\ntype %s = []json.RawMessage\n", typeName, g.schema.ID, typeName), nil
	}

	var buf strings.Builder
	var extras []string

	// If items is a union, generate the union type.
	if def.Items.Type == "union" {
		unionTypeName := typeName + "_Elem"
		unionCode, err := g.genUnionType(unionTypeName, def.Items.Refs, def.Items.Closed)
		if err != nil {
			return "", err
		}
		extras = append(extras, unionCode)
		fmt.Fprintf(&buf, "// %s is an array type defined in %s.\ntype %s = []%s\n", typeName, g.schema.ID, typeName, unionTypeName)
	} else {
		elemType, err := g.goType(def.Items, typeName, "elem", true, false)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&buf, "// %s is an array type defined in %s.\ntype %s = []%s\n", typeName, g.schema.ID, typeName, elemType)
	}

	result := buf.String()
	for _, extra := range extras {
		result = extra + "\n\n" + result
	}
	return result, nil
}

// genUnionDef generates a top-level union type (when a def itself is type "union").
func (g *fileGen) genUnionDef(defName string, def *lexicon.Def) (string, error) {
	typeName := g.typeName(defName)
	return g.genUnionType(typeName, def.Refs, def.Closed)
}

// exportFieldName converts a JSON field name to an exported Go field name.
func exportFieldName(name string) string {
	// Special cases for common abbreviations.
	switch name {
	case "uri":
		return "URI"
	case "cid":
		return "CID"
	case "did":
		return "DID"
	case "url":
		return "URL"
	}
	return capitalize(name)
}

func singleLineDesc(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 100 {
		return s[:97] + "..."
	}
	return s
}

func makeSet(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}

func sortedKeys(m map[string]*lexicon.Field) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sanitizeConstName(s string) string {
	// Replace non-alphanumeric chars with underscore.
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return capitalize(b.String())
}
