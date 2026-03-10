package lexicon

import (
	"fmt"
	"sort"
	"strings"
)

// Catalog holds parsed schemas and resolves cross-file references.
type Catalog struct {
	schemas map[string]*Schema // keyed by NSID
}

// NewCatalog creates an empty catalog.
func NewCatalog() *Catalog {
	return &Catalog{schemas: make(map[string]*Schema)}
}

// Add registers a schema in the catalog. Returns an error if the NSID is already registered.
func (c *Catalog) Add(s *Schema) error {
	if _, ok := c.schemas[s.ID]; ok {
		return fmt.Errorf("lexicon: duplicate schema %q", s.ID)
	}
	c.schemas[s.ID] = s
	return nil
}

// AddAll registers multiple schemas.
func (c *Catalog) AddAll(schemas []*Schema) error {
	for _, s := range schemas {
		if err := c.Add(s); err != nil {
			return err
		}
	}
	return nil
}

// Schema returns the schema for the given NSID, or nil.
func (c *Catalog) Schema(nsid string) *Schema {
	return c.schemas[nsid]
}

// Schemas returns all schemas sorted by NSID.
func (c *Catalog) Schemas() []*Schema {
	out := make([]*Schema, 0, len(c.schemas))
	for _, s := range c.schemas {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}

// Resolve validates that all references in all schemas point to existing definitions.
// Returns an error listing all unresolved references.
func (c *Catalog) Resolve() error {
	var errs []string
	for _, s := range c.schemas {
		for defName, def := range s.Defs {
			c.resolveInDef(s, defName, def, &errs)
		}
	}
	if len(errs) > 0 {
		sort.Strings(errs)
		return fmt.Errorf("lexicon: unresolved references:\n  %s", strings.Join(errs, "\n  "))
	}
	return nil
}

func (c *Catalog) resolveInDef(s *Schema, defName string, def *Def, errs *[]string) {
	ctx := s.ID + "#" + defName

	// Record
	if def.Record != nil {
		c.resolveInObject(s, ctx, def.Record, errs)
	}

	// Params
	if def.Parameters != nil {
		for fname, f := range def.Parameters.Properties {
			c.resolveInField(s, ctx+"."+fname, f, errs)
		}
	}

	// Input/Output
	if def.Input != nil && def.Input.Schema != nil {
		c.resolveInField(s, ctx+".input", def.Input.Schema, errs)
	}
	if def.Output != nil && def.Output.Schema != nil {
		c.resolveInField(s, ctx+".output", def.Output.Schema, errs)
	}

	// Message
	if def.Message != nil && def.Message.Schema != nil {
		c.resolveInField(s, ctx+".message", def.Message.Schema, errs)
	}

	// Inline object properties
	for fname, f := range def.Properties {
		c.resolveInField(s, ctx+"."+fname, f, errs)
	}

	// Inline union refs
	for _, ref := range def.Refs {
		c.resolveRef(s, ctx, ref, errs)
	}

	// Inline array items
	if def.Items != nil {
		c.resolveInField(s, ctx+".items", def.Items, errs)
	}

	// Inline ref
	if def.Ref != "" {
		c.resolveRef(s, ctx, def.Ref, errs)
	}
}

func (c *Catalog) resolveInObject(s *Schema, ctx string, obj *Object, errs *[]string) {
	for fname, f := range obj.Properties {
		c.resolveInField(s, ctx+"."+fname, f, errs)
	}
}

func (c *Catalog) resolveInField(s *Schema, ctx string, f *Field, errs *[]string) {
	switch f.Type {
	case "ref":
		c.resolveRef(s, ctx, f.Ref, errs)
	case "union":
		for _, ref := range f.Refs {
			c.resolveRef(s, ctx, ref, errs)
		}
	case "array":
		if f.Items != nil {
			c.resolveInField(s, ctx+"[]", f.Items, errs)
		}
	case "object":
		for fname, inner := range f.Properties {
			c.resolveInField(s, ctx+"."+fname, inner, errs)
		}
	}
}

// resolveRef checks that a ref string points to an existing definition.
func (c *Catalog) resolveRef(s *Schema, ctx, ref string, errs *[]string) {
	nsid, defName := SplitRef(s.ID, ref)
	target := c.schemas[nsid]
	if target == nil {
		*errs = append(*errs, fmt.Sprintf("%s: ref %q: schema %q not found", ctx, ref, nsid))
		return
	}
	if _, ok := target.Defs[defName]; !ok {
		*errs = append(*errs, fmt.Sprintf("%s: ref %q: def %q not found in %q", ctx, ref, defName, nsid))
	}
}

// SplitRef resolves a reference string relative to the given schema NSID.
// Returns the target NSID and def name.
//
//	"#replyRef"                        → (currentNSID, "replyRef")
//	"com.atproto.repo.defs#commitMeta" → ("com.atproto.repo.defs", "commitMeta")
//	"com.atproto.repo.strongRef"       → ("com.atproto.repo.strongRef", "main")
func SplitRef(currentNSID, ref string) (nsid, defName string) {
	if strings.HasPrefix(ref, "#") {
		return currentNSID, ref[1:]
	}
	if i := strings.LastIndex(ref, "#"); i >= 0 {
		return ref[:i], ref[i+1:]
	}
	return ref, "main"
}
