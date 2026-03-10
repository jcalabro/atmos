package lexgen

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jcalabro/atmos/lexicon"
)

// fieldInfo holds resolved info about a struct field for CBOR generation.
type fieldInfo struct {
	jsonName  string // the CBOR map key
	goField   string // Go struct field name
	goType    string // Go type string
	fieldType string // lexicon type (string, integer, boolean, etc.)
	required  bool
	nullable  bool
	field     *lexicon.Field // original field definition
}

// genCBORMethods generates MarshalCBOR/AppendCBOR and UnmarshalCBOR/UnmarshalCBORAt methods for a struct type.
func (g *fileGen) genCBORMethods(typeName string, obj *lexicon.Object, isRecord bool) (string, error) {
	g.addCBORImport()

	requiredSet := makeSet(obj.Required)
	nullableSet := makeSet(obj.Nullable)
	fieldNames := sortedKeys(obj.Properties)

	// Build field info list.
	var fields []fieldInfo
	// Record types always emit $type; non-record types emit it conditionally
	// so they can participate in unions (the union MarshalCBOR sets LexiconTypeID
	// before calling the inner type's MarshalCBOR).
	fields = append(fields, fieldInfo{
		jsonName:  "$type",
		goField:   "LexiconTypeID",
		goType:    "string",
		fieldType: "string",
		required:  isRecord,
	})
	for _, fname := range fieldNames {
		f := obj.Properties[fname]
		required := requiredSet[fname]
		nullable := nullableSet[fname]
		goTyp, err := g.goType(f, typeName, fname, required, nullable)
		if err != nil {
			return "", err
		}
		fields = append(fields, fieldInfo{
			jsonName:  fname,
			goField:   exportFieldName(fname),
			goType:    goTyp,
			fieldType: f.Type,
			required:  required,
			nullable:  nullable,
			field:     f,
		})
	}

	// Sort fields by DAG-CBOR key order.
	sort.Slice(fields, func(i, j int) bool {
		return cborKeyOrder(fields[i].jsonName, fields[j].jsonName) < 0
	})

	var buf strings.Builder

	// Generate precomputed key tokens.
	buf.WriteString("// Precomputed CBOR key tokens for " + typeName + ".\n")
	buf.WriteString("var (\n")
	for _, f := range fields {
		fmt.Fprintf(&buf, "\tcborKey_%s_%s = cbor.AppendTextKey(nil, %q)\n",
			typeName, sanitizeCBORKeyName(f.jsonName), f.jsonName)
	}
	buf.WriteString(")\n\n")

	// MarshalCBOR + AppendCBOR
	g.genMarshalCBOR(&buf, typeName, fields)

	buf.WriteString("\n\n")

	// UnmarshalCBOR + UnmarshalCBORAt
	g.genUnmarshalCBOR(&buf, typeName, fields)

	return buf.String(), nil
}

func (g *fileGen) genMarshalCBOR(buf *strings.Builder, typeName string, fields []fieldInfo) {
	// MarshalCBOR is a thin wrapper around AppendCBOR.
	fmt.Fprintf(buf, "func (s *%s) MarshalCBOR() ([]byte, error) {\n", typeName)
	buf.WriteString("\treturn s.AppendCBOR(make([]byte, 0, 256))\n")
	buf.WriteString("}\n\n")

	// AppendCBOR appends the CBOR encoding to buf and returns the extended buffer.
	fmt.Fprintf(buf, "func (s *%s) AppendCBOR(buf []byte) ([]byte, error) {\n", typeName)

	// Count required fields, then add optional field checks.
	requiredCount := 0
	var optionalFields []fieldInfo
	for _, f := range fields {
		if f.required && !f.nullable {
			requiredCount++
		} else {
			optionalFields = append(optionalFields, f)
		}
	}

	if len(optionalFields) > 0 {
		fmt.Fprintf(buf, "\tn := %d\n", requiredCount)
		for _, f := range optionalFields {
			fmt.Fprintf(buf, "\tif %s { n++ }\n", g.cborHasValue(f))
		}
		buf.WriteString("\tbuf = cbor.AppendMapHeader(buf, uint64(n))\n")
	} else {
		fmt.Fprintf(buf, "\tbuf = cbor.AppendMapHeader(buf, %d)\n", requiredCount)
	}

	// Encode fields in DAG-CBOR key order.
	for _, f := range fields {
		keyVar := fmt.Sprintf("cborKey_%s_%s", typeName, sanitizeCBORKeyName(f.jsonName))
		optional := !f.required || f.nullable

		if optional {
			fmt.Fprintf(buf, "\tif %s {\n", g.cborHasValue(f))
			fmt.Fprintf(buf, "\t\tbuf = append(buf, %s...)\n", keyVar)
			g.genCBOREncodeField(buf, f, "\t\t")
			buf.WriteString("\t}\n")
		} else {
			fmt.Fprintf(buf, "\tbuf = append(buf, %s...)\n", keyVar)
			g.genCBOREncodeField(buf, f, "\t")
		}
	}

	buf.WriteString("\treturn buf, nil\n")
	buf.WriteString("}")
}

func (g *fileGen) genUnmarshalCBOR(buf *strings.Builder, typeName string, fields []fieldInfo) {
	// UnmarshalCBOR is a thin wrapper around UnmarshalCBORAt.
	fmt.Fprintf(buf, "func (s *%s) UnmarshalCBOR(data []byte) error {\n", typeName)
	buf.WriteString("\t_, err := s.UnmarshalCBORAt(data, 0)\n")
	buf.WriteString("\treturn err\n")
	buf.WriteString("}\n\n")

	// UnmarshalCBORAt decodes from data starting at pos, returns new position.
	fmt.Fprintf(buf, "func (s *%s) UnmarshalCBORAt(data []byte, pos int) (int, error) {\n", typeName)
	buf.WriteString("\tcount, pos, err := cbor.ReadMapHeader(data, pos)\n")
	buf.WriteString("\tif err != nil { return 0, err }\n")
	buf.WriteString("\tfor i := uint64(0); i < count; i++ {\n")
	buf.WriteString("\t\tkey, newPos, err := cbor.ReadText(data, pos)\n")
	buf.WriteString("\t\tif err != nil { return 0, err }\n")
	buf.WriteString("\t\tpos = newPos\n")
	buf.WriteString("\t\tswitch key {\n")

	for _, f := range fields {
		fmt.Fprintf(buf, "\t\tcase %q:\n", f.jsonName)
		g.genCBORDecodeField(buf, f, "\t\t\t")
	}

	buf.WriteString("\t\tdefault:\n")
	buf.WriteString("\t\t\tpos, err = cbor.SkipValue(data, pos)\n")
	buf.WriteString("\t\t\tif err != nil { return 0, err }\n")
	buf.WriteString("\t\t}\n")
	buf.WriteString("\t}\n")
	buf.WriteString("\treturn pos, nil\n")
	buf.WriteString("}")
}

// cborHasValue returns a Go expression that checks if a field has a value.
func (g *fileGen) cborHasValue(f fieldInfo) string {
	if strings.HasPrefix(f.goType, "gt.Option[") || strings.HasPrefix(f.goType, "gt.Ref[") {
		return "s." + f.goField + ".HasVal()"
	}
	switch f.fieldType {
	case "string":
		return "s." + f.goField + " != \"\""
	case "integer":
		return "s." + f.goField + " != 0"
	case "boolean":
		return "s." + f.goField
	case "bytes":
		return "s." + f.goField + " != nil"
	case "array":
		return "len(s." + f.goField + ") > 0"
	default:
		// Struct types, etc — check for non-zero.
		return "true"
	}
}

// genCBOREncodeField generates CBOR encoding code for a single field value.
func (g *fileGen) genCBOREncodeField(buf *strings.Builder, f fieldInfo, indent string) {
	accessor := "s." + f.goField

	// Unwrap Option/Ref types.
	isOption := strings.HasPrefix(f.goType, "gt.Option[")
	isRef := strings.HasPrefix(f.goType, "gt.Ref[")
	if isOption || isRef {
		// For types that need AppendCBOR (pointer receiver), assign .Val() to a
		// temp variable so the result is addressable.
		if needsMarshalCBOR(g.resolveFieldType(f)) {
			fmt.Fprintf(buf, "%s{\n", indent)
			fmt.Fprintf(buf, "%s\tv := s.%s.Val()\n", indent, f.goField)
			accessor = "v"
			defer func() { fmt.Fprintf(buf, "%s}\n", indent) }()
			indent += "\t"
		} else {
			accessor = "s." + f.goField + ".Val()"
		}
	}

	// Handle nullable required fields: encode null if not set.
	if f.required && f.nullable {
		if isOption || isRef {
			fmt.Fprintf(buf, "%sif !s.%s.HasVal() {\n", indent, f.goField)
			fmt.Fprintf(buf, "%s\tbuf = cbor.AppendNull(buf)\n", indent)
			fmt.Fprintf(buf, "%s} else {\n", indent)
			g.genCBOREncodeValue(buf, f, accessor, indent+"\t")
			fmt.Fprintf(buf, "%s}\n", indent)
			return
		}
	}

	g.genCBOREncodeValue(buf, f, accessor, indent)
}

func (g *fileGen) genCBOREncodeValue(buf *strings.Builder, f fieldInfo, accessor, indent string) {
	// Resolve refs to primitive type aliases so they encode as primitives.
	ft := g.resolveFieldType(f)
	switch ft {
	case "string":
		fmt.Fprintf(buf, "%sbuf = cbor.AppendText(buf, %s)\n", indent, accessor)
	case "integer":
		fmt.Fprintf(buf, "%sbuf = cbor.AppendInt(buf, %s)\n", indent, accessor)
	case "boolean":
		fmt.Fprintf(buf, "%sbuf = cbor.AppendBool(buf, %s)\n", indent, accessor)
	case "bytes":
		fmt.Fprintf(buf, "%sbuf = cbor.AppendBytes(buf, %s)\n", indent, accessor)
	case "cid-link":
		fmt.Fprintf(buf, "%s{\n", indent)
		fmt.Fprintf(buf, "%s\tvar err error\n", indent)
		fmt.Fprintf(buf, "%s\tbuf, err = %s.AppendCBOR(buf)\n", indent, accessor)
		fmt.Fprintf(buf, "%s\tif err != nil { return nil, err }\n", indent)
		fmt.Fprintf(buf, "%s}\n", indent)
	case "blob":
		fmt.Fprintf(buf, "%s{\n", indent)
		fmt.Fprintf(buf, "%s\tvar err error\n", indent)
		fmt.Fprintf(buf, "%s\tbuf, err = %s.AppendCBOR(buf)\n", indent, accessor)
		fmt.Fprintf(buf, "%s\tif err != nil { return nil, err }\n", indent)
		fmt.Fprintf(buf, "%s}\n", indent)
	case "unknown", "null":
		// json.RawMessage — skip for CBOR (raw bytes pass-through not supported)
		fmt.Fprintf(buf, "%sbuf = cbor.AppendNull(buf)\n", indent)
	case "array":
		g.genCBOREncodeArray(buf, f, accessor, indent)
	case "object", "ref":
		if g.isRefToArrayDef(f) {
			// Ref to an array def — elements have their own AppendCBOR methods.
			fmt.Fprintf(buf, "%sbuf = cbor.AppendArrayHeader(buf, uint64(len(%s)))\n", indent, accessor)
			fmt.Fprintf(buf, "%sfor i := range %s {\n", indent, accessor)
			fmt.Fprintf(buf, "%s\tvar err error\n", indent)
			fmt.Fprintf(buf, "%s\tbuf, err = %s[i].AppendCBOR(buf)\n", indent, accessor)
			fmt.Fprintf(buf, "%s\tif err != nil { return nil, err }\n", indent)
			fmt.Fprintf(buf, "%s}\n", indent)
		} else {
			fmt.Fprintf(buf, "%s{\n", indent)
			fmt.Fprintf(buf, "%s\tvar err error\n", indent)
			fmt.Fprintf(buf, "%s\tbuf, err = %s.AppendCBOR(buf)\n", indent, accessor)
			fmt.Fprintf(buf, "%s\tif err != nil { return nil, err }\n", indent)
			fmt.Fprintf(buf, "%s}\n", indent)
		}
	case "union":
		fmt.Fprintf(buf, "%s{\n", indent)
		fmt.Fprintf(buf, "%s\tvar err error\n", indent)
		fmt.Fprintf(buf, "%s\tbuf, err = %s.AppendCBOR(buf)\n", indent, accessor)
		fmt.Fprintf(buf, "%s\tif err != nil { return nil, err }\n", indent)
		fmt.Fprintf(buf, "%s}\n", indent)
	default:
		fmt.Fprintf(buf, "%s// TODO: unsupported CBOR encode for type %q\n", indent, f.fieldType)
	}
}

func (g *fileGen) genCBOREncodeArray(buf *strings.Builder, f fieldInfo, accessor, indent string) {
	fmt.Fprintf(buf, "%sbuf = cbor.AppendArrayHeader(buf, uint64(len(%s)))\n", indent, accessor)
	fmt.Fprintf(buf, "%sfor _, item := range %s {\n", indent, accessor)

	elemType := "unknown"
	if f.field != nil && f.field.Items != nil {
		elemType = f.field.Items.Type
		// Resolve refs to primitive type aliases.
		if elemType == "ref" && f.field.Items.Ref != "" {
			elemType = g.resolvePrimitiveRef(f.field.Items.Ref, elemType)
		}
	}

	switch elemType {
	case "string":
		fmt.Fprintf(buf, "%s\tbuf = cbor.AppendText(buf, item)\n", indent)
	case "integer":
		fmt.Fprintf(buf, "%s\tbuf = cbor.AppendInt(buf, item)\n", indent)
	case "boolean":
		fmt.Fprintf(buf, "%s\tbuf = cbor.AppendBool(buf, item)\n", indent)
	case "bytes":
		fmt.Fprintf(buf, "%s\tbuf = cbor.AppendBytes(buf, item)\n", indent)
	case "ref", "object", "union":
		fmt.Fprintf(buf, "%s\tvar err error\n", indent)
		fmt.Fprintf(buf, "%s\tbuf, err = item.AppendCBOR(buf)\n", indent)
		fmt.Fprintf(buf, "%s\tif err != nil { return nil, err }\n", indent)
	default:
		fmt.Fprintf(buf, "%s\t_ = item // TODO: unsupported array element CBOR encode\n", indent)
	}

	fmt.Fprintf(buf, "%s}\n", indent)
}

// genCBORDecodeField generates CBOR decoding code for a single field.
// Error returns use "return 0, err" for UnmarshalCBORAt.
func (g *fileGen) genCBORDecodeField(buf *strings.Builder, f fieldInfo, indent string) {
	isOption := strings.HasPrefix(f.goType, "gt.Option[")

	// Resolve refs to primitive type aliases so they decode as primitives.
	ft := g.resolveFieldType(f)

	switch ft {
	case "string":
		if isOption {
			fmt.Fprintf(buf, "%sif cbor.IsNull(data, pos) {\n", indent)
			fmt.Fprintf(buf, "%s\tpos++\n", indent)
			fmt.Fprintf(buf, "%s} else {\n", indent)
			fmt.Fprintf(buf, "%s\tvar v string\n", indent)
			fmt.Fprintf(buf, "%s\tv, pos, err = cbor.ReadText(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s\ts.%s = gt.Some(v)\n", indent, f.goField)
			fmt.Fprintf(buf, "%s}\n", indent)
		} else {
			fmt.Fprintf(buf, "%ss.%s, pos, err = cbor.ReadText(data, pos)\n", indent, f.goField)
			fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
		}
	case "integer":
		if isOption {
			fmt.Fprintf(buf, "%sif cbor.IsNull(data, pos) {\n", indent)
			fmt.Fprintf(buf, "%s\tpos++\n", indent)
			fmt.Fprintf(buf, "%s} else {\n", indent)
			fmt.Fprintf(buf, "%s\tvar v int64\n", indent)
			fmt.Fprintf(buf, "%s\tv, pos, err = cbor.ReadInt(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s\ts.%s = gt.Some(v)\n", indent, f.goField)
			fmt.Fprintf(buf, "%s}\n", indent)
		} else {
			fmt.Fprintf(buf, "%ss.%s, pos, err = cbor.ReadInt(data, pos)\n", indent, f.goField)
			fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
		}
	case "boolean":
		if isOption {
			fmt.Fprintf(buf, "%sif cbor.IsNull(data, pos) {\n", indent)
			fmt.Fprintf(buf, "%s\tpos++\n", indent)
			fmt.Fprintf(buf, "%s} else {\n", indent)
			fmt.Fprintf(buf, "%s\tvar v bool\n", indent)
			fmt.Fprintf(buf, "%s\tv, pos, err = cbor.ReadBool(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s\ts.%s = gt.Some(v)\n", indent, f.goField)
			fmt.Fprintf(buf, "%s}\n", indent)
		} else {
			fmt.Fprintf(buf, "%ss.%s, pos, err = cbor.ReadBool(data, pos)\n", indent, f.goField)
			fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
		}
	case "bytes":
		fmt.Fprintf(buf, "%ss.%s, pos, err = cbor.ReadBytes(data, pos)\n", indent, f.goField)
		fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
	case "cid-link":
		if isOption {
			fmt.Fprintf(buf, "%sif cbor.IsNull(data, pos) {\n", indent)
			fmt.Fprintf(buf, "%s\tpos++\n", indent)
			fmt.Fprintf(buf, "%s} else {\n", indent)
			fmt.Fprintf(buf, "%s\tvar v %s\n", indent, g.sharedType("LexCIDLink"))
			fmt.Fprintf(buf, "%s\tpos, err = v.UnmarshalCBORAt(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s\ts.%s = gt.Some(v)\n", indent, f.goField)
			fmt.Fprintf(buf, "%s}\n", indent)
		} else {
			fmt.Fprintf(buf, "%spos, err = s.%s.UnmarshalCBORAt(data, pos)\n", indent, f.goField)
			fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
		}
	case "blob":
		if isOption {
			fmt.Fprintf(buf, "%sif cbor.IsNull(data, pos) {\n", indent)
			fmt.Fprintf(buf, "%s\tpos++\n", indent)
			fmt.Fprintf(buf, "%s} else {\n", indent)
			fmt.Fprintf(buf, "%s\tvar v %s\n", indent, g.sharedType("LexBlob"))
			fmt.Fprintf(buf, "%s\tpos, err = v.UnmarshalCBORAt(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s\ts.%s = gt.Some(v)\n", indent, f.goField)
			fmt.Fprintf(buf, "%s}\n", indent)
		} else {
			fmt.Fprintf(buf, "%spos, err = s.%s.UnmarshalCBORAt(data, pos)\n", indent, f.goField)
			fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
		}
	case "unknown", "null":
		fmt.Fprintf(buf, "%spos, err = cbor.SkipValue(data, pos)\n", indent)
		fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
	case "array":
		g.genCBORDecodeArray(buf, f, indent)
	case "object", "ref", "union":
		if elemType := g.refToArrayElemType(f); elemType != "" {
			// Ref to an array def — decode each element via UnmarshalCBORAt.
			fmt.Fprintf(buf, "%s{\n", indent)
			fmt.Fprintf(buf, "%s\tarrLen, newPos, err := cbor.ReadArrayHeader(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s\tpos = newPos\n", indent)
			fmt.Fprintf(buf, "%s\ts.%s = make(%s, arrLen)\n", indent, f.goField, f.goType)
			fmt.Fprintf(buf, "%s\tfor i := range arrLen {\n", indent)
			fmt.Fprintf(buf, "%s\t\tvar elem %s\n", indent, elemType)
			fmt.Fprintf(buf, "%s\t\tpos, err = elem.UnmarshalCBORAt(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s\t\ts.%s[i] = elem\n", indent, f.goField)
			fmt.Fprintf(buf, "%s\t}\n", indent)
			fmt.Fprintf(buf, "%s}\n", indent)
		} else {
			g.genCBORDecodeNested(buf, f, indent, isOption)
		}
	default:
		fmt.Fprintf(buf, "%spos, err = cbor.SkipValue(data, pos)\n", indent)
		fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
	}
}

// genCBORDecodeNested generates decode code for a nested struct (object, ref, or union).
func (g *fileGen) genCBORDecodeNested(buf *strings.Builder, f fieldInfo, indent string, isOption bool) {
	if isOption {
		innerType := f.goType[len("gt.Option[") : len(f.goType)-1]
		fmt.Fprintf(buf, "%sif cbor.IsNull(data, pos) {\n", indent)
		fmt.Fprintf(buf, "%s\tpos++\n", indent)
		fmt.Fprintf(buf, "%s} else {\n", indent)
		fmt.Fprintf(buf, "%s\tvar v %s\n", indent, innerType)
		fmt.Fprintf(buf, "%s\tpos, err = v.UnmarshalCBORAt(data, pos)\n", indent)
		fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
		fmt.Fprintf(buf, "%s\ts.%s = gt.Some(v)\n", indent, f.goField)
		fmt.Fprintf(buf, "%s}\n", indent)
	} else {
		fmt.Fprintf(buf, "%spos, err = s.%s.UnmarshalCBORAt(data, pos)\n", indent, f.goField)
		fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
	}
}

func (g *fileGen) genCBORDecodeArray(buf *strings.Builder, f fieldInfo, indent string) {
	fmt.Fprintf(buf, "%s{\n", indent)
	fmt.Fprintf(buf, "%s\tarrLen, newPos, err := cbor.ReadArrayHeader(data, pos)\n", indent)
	fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
	fmt.Fprintf(buf, "%s\tpos = newPos\n", indent)

	elemType := "unknown"
	if f.field != nil && f.field.Items != nil {
		elemType = f.field.Items.Type
		// Resolve refs to primitive type aliases.
		if elemType == "ref" && f.field.Items.Ref != "" {
			elemType = g.resolvePrimitiveRef(f.field.Items.Ref, elemType)
		}
	}

	// Determine the Go element type for make().
	goElemType := "any"
	if f.field != nil && f.field.Items != nil {
		// Strip the []
		if strings.HasPrefix(f.goType, "[]") {
			goElemType = f.goType[2:]
		} else if strings.HasPrefix(f.goType, "gt.Option[[]") {
			goElemType = f.goType[len("gt.Option[[]") : len(f.goType)-1]
		}
	}

	fmt.Fprintf(buf, "%s\ts.%s = make([]%s, arrLen)\n", indent, f.goField, goElemType)
	fmt.Fprintf(buf, "%s\tfor idx := range arrLen {\n", indent)

	switch elemType {
	case "string":
		fmt.Fprintf(buf, "%s\t\ts.%s[idx], pos, err = cbor.ReadText(data, pos)\n", indent, f.goField)
		fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
	case "integer":
		fmt.Fprintf(buf, "%s\t\ts.%s[idx], pos, err = cbor.ReadInt(data, pos)\n", indent, f.goField)
		fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
	case "boolean":
		fmt.Fprintf(buf, "%s\t\ts.%s[idx], pos, err = cbor.ReadBool(data, pos)\n", indent, f.goField)
		fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
	case "bytes":
		fmt.Fprintf(buf, "%s\t\ts.%s[idx], pos, err = cbor.ReadBytes(data, pos)\n", indent, f.goField)
		fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
	case "ref", "object", "union":
		fmt.Fprintf(buf, "%s\t\tpos, err = s.%s[idx].UnmarshalCBORAt(data, pos)\n", indent, f.goField)
		fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
	default:
		fmt.Fprintf(buf, "%s\t\t_ = idx\n", indent)
		fmt.Fprintf(buf, "%s\t\tpos, err = cbor.SkipValue(data, pos)\n", indent)
		fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
	}

	fmt.Fprintf(buf, "%s\t}\n", indent)
	fmt.Fprintf(buf, "%s}\n", indent)
}

// genCBORUnion generates MarshalCBOR/AppendCBOR and UnmarshalCBOR/UnmarshalCBORAt for a union type.
func (g *fileGen) genCBORUnion(typeName string, resolved []unionRefInfo, closed bool) (string, error) {
	g.imports["fmt"] = true
	g.addCBORImport()

	var buf strings.Builder

	// MarshalCBOR — thin wrapper.
	fmt.Fprintf(&buf, "func (u %s) MarshalCBOR() ([]byte, error) {\n", typeName)
	buf.WriteString("\treturn u.AppendCBOR(make([]byte, 0, 256))\n")
	buf.WriteString("}\n\n")

	// AppendCBOR
	fmt.Fprintf(&buf, "func (u %s) AppendCBOR(buf []byte) ([]byte, error) {\n", typeName)
	for _, r := range resolved {
		fmt.Fprintf(&buf, "\tif u.%s.HasVal() {\n", r.fieldName)
		fmt.Fprintf(&buf, "\t\tv := *u.%s.Val()\n", r.fieldName)
		fmt.Fprintf(&buf, "\t\tv.LexiconTypeID = %q\n", r.typeID)
		buf.WriteString("\t\treturn v.AppendCBOR(buf)\n")
		buf.WriteString("\t}\n")
	}
	if !closed {
		buf.WriteString("\tif u.Unknown.HasVal() {\n")
		buf.WriteString("\t\treturn append(buf, u.Unknown.Val().RawCBOR...), nil\n")
		buf.WriteString("\t}\n")
	}
	fmt.Fprintf(&buf, "\treturn nil, fmt.Errorf(\"cannot marshal empty union %s\")\n", typeName)
	buf.WriteString("}\n\n")

	// UnmarshalCBOR — thin wrapper.
	fmt.Fprintf(&buf, "func (u *%s) UnmarshalCBOR(data []byte) error {\n", typeName)
	buf.WriteString("\t_, err := u.UnmarshalCBORAt(data, 0)\n")
	buf.WriteString("\treturn err\n")
	buf.WriteString("}\n\n")

	// UnmarshalCBORAt
	fmt.Fprintf(&buf, "func (u *%s) UnmarshalCBORAt(data []byte, pos int) (int, error) {\n", typeName)
	buf.WriteString("\ttyp, err := cbor.PeekTypeAt(data, pos)\n")
	buf.WriteString("\tif err != nil { return 0, err }\n")
	buf.WriteString("\tswitch typ {\n")
	for _, r := range resolved {
		fmt.Fprintf(&buf, "\tcase %q:\n", r.typeID)
		fmt.Fprintf(&buf, "\t\tvar v %s\n", r.goType)
		buf.WriteString("\t\tpos, err = v.UnmarshalCBORAt(data, pos)\n")
		buf.WriteString("\t\tif err != nil { return 0, err }\n")
		fmt.Fprintf(&buf, "\t\tu.%s = gt.SomeRef(v)\n", r.fieldName)
		buf.WriteString("\t\treturn pos, nil\n")
	}
	buf.WriteString("\tdefault:\n")
	if closed {
		fmt.Fprintf(&buf, "\t\treturn 0, fmt.Errorf(\"unknown type %%q in union %s\", typ)\n", typeName)
	} else {
		// Preserve the raw CBOR bytes for unknown variants.
		buf.WriteString("\t\tstartPos := pos\n")
		buf.WriteString("\t\tpos, err = cbor.SkipValue(data, pos)\n")
		buf.WriteString("\t\tif err != nil { return 0, err }\n")
		buf.WriteString("\t\traw := make([]byte, pos-startPos)\n")
		buf.WriteString("\t\tcopy(raw, data[startPos:pos])\n")
		fmt.Fprintf(&buf, "\t\tu.Unknown = gt.SomeRef(%s{Type: typ, RawCBOR: raw})\n", g.sharedType("UnknownUnionVariant"))
		buf.WriteString("\t\treturn pos, nil\n")
	}
	buf.WriteString("\t}\n")
	buf.WriteString("}")

	return buf.String(), nil
}

func (g *fileGen) addCBORImport() {
	g.imports["\"github.com/jcalabro/atmos/cbor\""] = true
}

// cborKeyOrder compares two string keys by DAG-CBOR sort order
// (shorter CBOR encoding first, then lexicographic).
// Mirrors cbor.CompareCBORKeys.
func cborKeyOrder(a, b string) int {
	if len(a) != len(b) {
		return len(a) - len(b)
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func sanitizeCBORKeyName(s string) string {
	if s == "$type" {
		return "dollar_type"
	}
	// Replace $ and other non-identifier chars.
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// needsMarshalCBOR returns true if the given lexicon field type requires
// calling AppendCBOR (i.e. it's a struct type with pointer receiver methods).
func needsMarshalCBOR(fieldType string) bool {
	switch fieldType {
	case "object", "ref", "union", "blob", "cid-link":
		return true
	}
	return false
}

// resolveFieldType returns the effective lexicon type for a field, resolving
// refs to primitive type aliases (string, integer, boolean) to their underlying type.
func (g *fileGen) resolveFieldType(f fieldInfo) string {
	if f.fieldType != "ref" || f.field == nil || f.field.Ref == "" {
		return f.fieldType
	}
	return g.resolvePrimitiveRef(f.field.Ref, f.fieldType)
}

// refToArrayElemType checks if a ref field points to an array def and returns
// the Go element type name. Returns "" if the field is not a ref-to-array.
func (g *fileGen) refToArrayElemType(f fieldInfo) string {
	if f.fieldType != "ref" || f.field == nil || f.field.Ref == "" {
		return ""
	}
	nsid, defName := lexicon.SplitRef(g.schema.ID, f.field.Ref)
	s := g.cat.Schema(nsid)
	if s == nil {
		return ""
	}
	def, ok := s.Defs[defName]
	if !ok || def.Type != "array" {
		return ""
	}
	// The Go type for the field is the array type alias (e.g. ActorDefs_Preferences).
	// For union items, the element type is {GoType}_Elem (defined by genArrayDef).
	if def.Items != nil && def.Items.Type == "union" {
		return f.goType + "_Elem"
	}
	// For non-union items, the element type is just the Go type of the items.
	// These are primitive or ref types that don't need AppendCBOR/AppendJSON.
	return ""
}

// isRefToArrayDef returns true if the field is a ref pointing to an array def
// with elements that have marshal methods.
func (g *fileGen) isRefToArrayDef(f fieldInfo) bool {
	return g.refToArrayElemType(f) != ""
}

// resolvePrimitiveRef checks if a ref points to a primitive type alias and returns
// the primitive type if so, otherwise returns fallback.
func (g *fileGen) resolvePrimitiveRef(ref, fallback string) string {
	nsid, defName := lexicon.SplitRef(g.schema.ID, ref)
	s := g.cat.Schema(nsid)
	if s != nil {
		if def, ok := s.Defs[defName]; ok {
			switch def.Type {
			case "string", "integer", "boolean":
				return def.Type
			}
		}
	}
	return fallback
}
