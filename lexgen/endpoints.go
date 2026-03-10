package lexgen

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jcalabro/atmos/lexicon"
)

// genQuery generates a client function for an XRPC query (GET).
func (g *fileGen) genQuery(defName string, def *lexicon.Def) (string, error) {
	typeName := g.typeName(defName)
	var buf strings.Builder
	var extras []string

	// Generate output type if present.
	if def.Output != nil && def.Output.Schema != nil {
		outCode, err := g.genEndpointSchema(typeName+"_Output", def.Output.Schema)
		if err != nil {
			return "", err
		}
		extras = append(extras, outCode...)
	}

	// Build function signature.
	g.imports["context"] = true
	g.imports["github.com/jcalabro/atmos/xrpc"] = true

	params := g.collectParams(def.Parameters)
	hasOutput := def.Output != nil && def.Output.Schema != nil
	hasBinaryOutput := def.Output != nil && def.Output.Encoding != "" && def.Output.Schema == nil

	comment := fmt.Sprintf("// %s calls the XRPC query %q.", typeName, g.schema.ID)
	if def.Desc != "" {
		comment = fmt.Sprintf("// %s calls the XRPC query %q.\n//\n// %s", typeName, g.schema.ID, def.Desc)
	}
	buf.WriteString(comment + "\n")

	// Function params.
	funcParams := "ctx context.Context, c *xrpc.Client"
	for _, p := range params {
		funcParams += ", " + p.name + " " + p.goType
	}

	if hasOutput {
		fmt.Fprintf(&buf, "func %s(%s) (*%s_Output, error) {\n", typeName, funcParams, typeName)
	} else if hasBinaryOutput {
		fmt.Fprintf(&buf, "func %s(%s) ([]byte, error) {\n", typeName, funcParams)
	} else {
		fmt.Fprintf(&buf, "func %s(%s) error {\n", typeName, funcParams)
	}

	// Build params map.
	if len(params) > 0 {
		buf.WriteString("\tparams := map[string]any{}\n")
		for _, p := range params {
			if p.required {
				fmt.Fprintf(&buf, "\tparams[%q] = %s\n", p.jsonName, p.name)
			} else {
				buf.WriteString(g.paramZeroCheck(p))
			}
		}
	}

	// Make the call.
	paramsArg := "nil"
	if len(params) > 0 {
		paramsArg = "params"
	}

	if hasOutput {
		fmt.Fprintf(&buf, "\tvar out %s_Output\n", typeName)
		fmt.Fprintf(&buf, "\treturn &out, c.Query(ctx, %q, %s, &out)\n", g.schema.ID, paramsArg)
	} else if hasBinaryOutput {
		fmt.Fprintf(&buf, "\treturn c.QueryRaw(ctx, %q, %s)\n", g.schema.ID, paramsArg)
	} else {
		fmt.Fprintf(&buf, "\treturn c.Query(ctx, %q, %s, nil)\n", g.schema.ID, paramsArg)
	}
	buf.WriteString("}")

	if errConsts := g.genErrorConstants(typeName, def.Errors); errConsts != "" {
		extras = append(extras, errConsts)
	}

	result := buf.String()
	for _, extra := range extras {
		result = extra + "\n\n" + result
	}
	return result, nil
}

// genProcedure generates a client function for an XRPC procedure (POST).
func (g *fileGen) genProcedure(defName string, def *lexicon.Def) (string, error) {
	typeName := g.typeName(defName)
	var buf strings.Builder
	var extras []string

	// Generate input type if present.
	hasInput := def.Input != nil && def.Input.Schema != nil
	if hasInput {
		inCode, err := g.genEndpointSchema(typeName+"_Input", def.Input.Schema)
		if err != nil {
			return "", err
		}
		extras = append(extras, inCode...)
	}

	// Generate output type if present.
	hasOutput := def.Output != nil && def.Output.Schema != nil
	if hasOutput {
		outCode, err := g.genEndpointSchema(typeName+"_Output", def.Output.Schema)
		if err != nil {
			return "", err
		}
		extras = append(extras, outCode...)
	}

	g.imports["context"] = true
	g.imports["github.com/jcalabro/atmos/xrpc"] = true

	comment := fmt.Sprintf("// %s calls the XRPC procedure %q.", typeName, g.schema.ID)
	if def.Desc != "" {
		comment = fmt.Sprintf("// %s calls the XRPC procedure %q.\n//\n// %s", typeName, g.schema.ID, def.Desc)
	}
	buf.WriteString(comment + "\n")

	// Check for binary input (blob upload, CAR import, etc.).
	isBlob := def.Input != nil && def.Input.Encoding != "" && def.Input.Schema == nil

	funcParams := "ctx context.Context, c *xrpc.Client"
	if isBlob {
		g.imports["io"] = true
		funcParams += ", contentType string, body io.Reader"
	} else if hasInput {
		funcParams += ", input *" + typeName + "_Input"
	}

	if hasOutput {
		fmt.Fprintf(&buf, "func %s(%s) (*%s_Output, error) {\n", typeName, funcParams, typeName)
	} else {
		fmt.Fprintf(&buf, "func %s(%s) error {\n", typeName, funcParams)
	}

	if isBlob {
		if hasOutput {
			fmt.Fprintf(&buf, "\tvar out %s_Output\n", typeName)
			fmt.Fprintf(&buf, "\treturn &out, c.Do(ctx, \"POST\", %q, contentType, nil, body, &out)\n", g.schema.ID)
		} else {
			fmt.Fprintf(&buf, "\treturn c.Do(ctx, \"POST\", %q, contentType, nil, body, nil)\n", g.schema.ID)
		}
	} else {
		inArg := "nil"
		if hasInput {
			inArg = "input"
		}
		if hasOutput {
			fmt.Fprintf(&buf, "\tvar out %s_Output\n", typeName)
			fmt.Fprintf(&buf, "\treturn &out, c.Procedure(ctx, %q, %s, &out)\n", g.schema.ID, inArg)
		} else {
			fmt.Fprintf(&buf, "\treturn c.Procedure(ctx, %q, %s, nil)\n", g.schema.ID, inArg)
		}
	}
	buf.WriteString("}")

	if errConsts := g.genErrorConstants(typeName, def.Errors); errConsts != "" {
		extras = append(extras, errConsts)
	}

	result := buf.String()
	for _, extra := range extras {
		result = extra + "\n\n" + result
	}
	return result, nil
}

// genSubscription generates types for a subscription endpoint.
// We don't generate a client function here since subscriptions use WebSocket.
func (g *fileGen) genSubscription(defName string, def *lexicon.Def) (string, error) {
	if def.Message == nil || def.Message.Schema == nil {
		return "", nil
	}
	typeName := g.typeName(defName)

	// Generate the message union type.
	if def.Message.Schema.Type == "union" {
		return g.genUnionType(typeName+"_Message", def.Message.Schema.Refs, def.Message.Schema.Closed)
	}
	return "", nil
}

// genEndpointSchema generates types from an endpoint's input/output schema.
// Returns multiple code sections (the main type + any nested types).
func (g *fileGen) genEndpointSchema(typeName string, f *lexicon.Field) ([]string, error) {
	if f.Type == "ref" {
		// Output is a ref — no need to generate a type, just alias it.
		goTyp, err := g.resolveRefType(f.Ref)
		if err != nil {
			return nil, err
		}
		return []string{fmt.Sprintf("// %s is an alias for %s.\ntype %s = %s", typeName, goTyp, typeName, goTyp)}, nil
	}

	if f.Type != "object" || f.Properties == nil {
		return nil, nil
	}

	obj := &lexicon.Object{
		Properties: f.Properties,
		Required:   f.Required,
		Nullable:   f.Nullable,
	}
	structCode, extras, err := g.genStructBody(typeName, obj, false)
	if err != nil {
		return nil, err
	}

	// Generate CBOR marshal/unmarshal.
	cborCode, err := g.genCBORMethods(typeName, obj, false)
	if err != nil {
		return nil, err
	}

	// Generate JSON marshal/unmarshal.
	jsonCode, err := g.genJSONMethods(typeName, obj, false)
	if err != nil {
		return nil, err
	}

	result := []string{structCode}
	result = append(result, extras...)
	result = append(result, cborCode, jsonCode)
	return result, nil
}

type paramInfo struct {
	name     string // Go param name
	jsonName string // JSON param name
	goType   string
	required bool
}

func (g *fileGen) collectParams(params *lexicon.Params) []paramInfo {
	if params == nil || len(params.Properties) == 0 {
		return nil
	}

	requiredSet := makeSet(params.Required)

	// Sort params alphabetically.
	names := make([]string, 0, len(params.Properties))
	for name := range params.Properties {
		names = append(names, name)
	}
	sort.Strings(names)

	var result []paramInfo
	for _, name := range names {
		f := params.Properties[name]
		required := requiredSet[name]
		result = append(result, paramInfo{
			name:     goParamName(name),
			jsonName: name,
			goType:   paramGoType(f),
			required: required,
		})
	}
	return result
}

func (g *fileGen) paramZeroCheck(p paramInfo) string {
	switch p.goType {
	case "string":
		return fmt.Sprintf("\tif %s != \"\" { params[%q] = %s }\n", p.name, p.jsonName, p.name)
	case "int64":
		return fmt.Sprintf("\tif %s != 0 { params[%q] = %s }\n", p.name, p.jsonName, p.name)
	case "bool":
		return fmt.Sprintf("\tif %s { params[%q] = %s }\n", p.name, p.jsonName, p.name)
	case "[]string":
		return fmt.Sprintf("\tif len(%s) > 0 { params[%q] = %s }\n", p.name, p.jsonName, p.name)
	default:
		return fmt.Sprintf("\tparams[%q] = %s\n", p.jsonName, p.name)
	}
}

// goParamName converts a JSON parameter name to a valid Go parameter name.
func goParamName(name string) string {
	// Lowercase first letter for unexported param name.
	if name == "" {
		return name
	}
	// Avoid Go keywords and predeclared identifiers.
	switch name {
	case "break":
		return "brk"
	case "case":
		return "cas"
	case "chan":
		return "ch"
	case "const":
		return "cnst"
	case "continue":
		return "cont"
	case "default":
		return "def"
	case "defer":
		return "dfr"
	case "else":
		return "els"
	case "fallthrough":
		return "ft"
	case "for":
		return "fr"
	case "func":
		return "fn"
	case "go":
		return "g"
	case "goto":
		return "gt"
	case "if":
		return "cond"
	case "import":
		return "imp"
	case "interface":
		return "iface"
	case "map":
		return "mp"
	case "package":
		return "pkg"
	case "range":
		return "rng"
	case "return":
		return "ret"
	case "select":
		return "sel"
	case "struct":
		return "st"
	case "switch":
		return "sw"
	case "type":
		return "typ"
	case "var":
		return "v"
	}
	return name
}

// genErrorConstants generates string constants for endpoint error names.
func (g *fileGen) genErrorConstants(typeName string, errors []lexicon.ErrorDef) string {
	if len(errors) == 0 {
		return ""
	}
	var buf strings.Builder
	fmt.Fprintf(&buf, "// Error name constants for %s.\n", typeName)
	buf.WriteString("const (\n")
	for _, e := range errors {
		constName := fmt.Sprintf("Err%s_%s", typeName, e.Name)
		if e.Desc != "" {
			fmt.Fprintf(&buf, "\t%s = %q // %s\n", constName, e.Name, singleLineDesc(e.Desc))
		} else {
			fmt.Fprintf(&buf, "\t%s = %q\n", constName, e.Name)
		}
	}
	buf.WriteString(")")
	return buf.String()
}

func (g *fileGen) genToken(defName string, def *lexicon.Def) string {
	typeName := g.typeName(defName)
	fullRef := g.schema.ID + "#" + defName
	comment := ""
	if def.Desc != "" {
		comment = " // " + singleLineDesc(def.Desc)
	}
	return fmt.Sprintf("\t%s = %q%s\n", typeName, fullRef, comment)
}

func (g *fileGen) genNSID(defName string, def *lexicon.Def) string {
	typeName := g.typeName(defName)
	comment := ""
	if def.Desc != "" {
		comment = " // " + singleLineDesc(def.Desc)
	}
	return fmt.Sprintf("\tNSID%s = %q%s\n", typeName, g.schema.ID, comment)
}
