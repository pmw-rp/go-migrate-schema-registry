package main

import (
	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/sr"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"
)

type State struct {
	SubjectSchemas       []sr.SubjectSchema       `yaml:"subjectSchemas"`
	CompatibilityResults []sr.CompatibilityResult `yaml:"compatibilityResults"`
	SoftDeletions        []sr.SubjectVersion      `yaml:"softDeletions"`
}

func (s *State) validate() {
	index := make(map[sr.SubjectVersion]int)
	for i, subjectSchema := range s.SubjectSchemas {
		for _, schemaReference := range subjectSchema.References {
			reference := sr.SubjectVersion{
				Subject: schemaReference.Subject,
				Version: schemaReference.Version,
			}
			_, ok := index[reference]
			if !ok {
				panic(fmt.Sprintf("Should have found a reference for %v", reference))
			}
		}
		reference := sr.SubjectVersion{
			Subject: subjectSchema.Subject,
			Version: subjectSchema.Version,
		}
		_, ok := index[reference]
		if ok {
			panic(fmt.Sprintf("Should not have found a reference for %v", reference))
		}
		index[reference] = i
	}
}

// We can't purely sort on ID, since that can't be guaranteed to respect references.
//
// There are two types of reference: explicit references, that are noted in the sr.SubjectSchema, and implicit references,
// that are based on usage in the schema. We need to respect both.
//
// This sort process builds a graph of reference dependencies, and assigns each node a "level", denoting its
// distance from the root (level 0 = root node without dependencies, level 1 = dependent on a root node,
// 2 = dependent on a level 1 node, etc
func (s *State) sort() {

	type node struct {
		level         int
		reference     *sr.SchemaReference
		subjectSchema *sr.SubjectSchema
		dependents    []*node
		dependsOn     []*node
	}

	nodes := make(map[sr.SchemaReference]*node)
	// First pass to create the node map
	for _, schema := range s.SubjectSchemas {
		reference := sr.SchemaReference{Subject: schema.Subject, Name: schema.Subject, Version: schema.Version}
		nodes[reference] = &node{reference: &reference, subjectSchema: &schema, level: -1}
	}

	// Second pass to wire up dependencies
	for _, n := range nodes {
		refs := s.getReferences(*n.subjectSchema)
		for ref := range refs {
			n.dependsOn = append(n.dependsOn, nodes[ref])
			nodes[ref].dependents = append(nodes[ref].dependents, n)
		}
	}

	// At this point, we have a map of nodes
	// Each node has a full list of dependencies (both explicit and implicit)
	// We start enumerating the levels of the tree(s), so that we can use that in ordering

	changed := 0 // variable to find how many nodes are assigned a level in this round
	// a first enumeration pass, in which we identify level 0 nodes (no parent)
	for _, n := range nodes {
		if n.dependsOn == nil {
			n.level = 0
			changed++
		}
	}
	level := 0
	for changed > 0 { // while some nodes changed on the last pass, we may have nodes not yet assigned
		changed = 0 // reset the counter to count how many nodes get a label this time round
		for _, n := range nodes {
			if n.level == level {
				for _, ref := range n.dependents {
					nodes[*ref.reference].level = level + 1
					changed++
				}
			}
		}
		level++
	}

	// At this point, we've assigned a level to each node, so we're fully aware of dependencies - we just need to sort
	// Store the nodes into a slice and sort them first by level, and then by schema ID
	nodeSlice := slices.Collect(maps.Values(nodes))
	slices.SortFunc(nodeSlice, func(a, b *node) int {
		comparison := a.level - b.level
		if comparison == 0 {
			comparison = a.subjectSchema.ID - b.subjectSchema.ID
		}
		return comparison
	})

	// Replace the schemas with the newly ordered ones
	for i, node := range nodeSlice {
		s.SubjectSchemas[i] = *node.subjectSchema
	}

	// We're done.
}

func (s *State) getReferences(ss sr.SubjectSchema) map[sr.SchemaReference]bool {
	refs := make(map[sr.SchemaReference]bool)
	// Firstly, add any explicit references that are already in the metadata
	for _, reference := range ss.References {
		translatedReference := sr.SchemaReference{
			Subject: reference.Subject,
			Name:    reference.Subject,
			Version: reference.Version,
		}
		refs[translatedReference] = true
	}
	// Next, add any type specific references that are found in the schema text
	switch ss.Type {
	case sr.TypeProtobuf:
		importRegexString := `import "(.+?)";`
		importRegex := regexp.MustCompile(importRegexString)
		matches := importRegex.FindAllStringSubmatch(ss.Schema.Schema, -1)
		for _, match := range matches {
			if len(match) > 1 {
				for i := 1; i < len(match); i++ {
					versions := s.findAllVersionsForSubject(match[i])
					for _, version := range versions {
						ref := sr.SchemaReference{
							Subject: match[i],
							Name:    match[i],
							Version: version,
						}
						refs[ref] = true
					}

				}
			}
		}
	case sr.TypeAvro:
		result := make(map[string]interface{})
		err := json.Unmarshal([]byte(ss.Schema.Schema), &result)
		if err != nil {
			panic(err)
		}
		var self string
		_, ok := result["namespace"]
		if !ok {
			self = result["name"].(string)
		} else {
			self = result["namespace"].(string) + "." + result["name"].(string)
		}
		s.findTypeKeys(result, &refs, self)
	case sr.TypeJSON:
		panic("JSON schema not yet supported for sorting - contact Redpanda CSE team")
	}

	return refs
}

func (s *State) findAllVersionsForSubject(subject string) []int {
	versions := make(map[int]bool)
	for _, ss := range s.SubjectSchemas {
		if ss.Subject == subject {
			versions[ss.Version] = true
		}
	}
	return slices.Collect(maps.Keys(versions))
}

func (s *State) findTypeKeys(v interface{}, refs *map[sr.SchemaReference]bool, self string) {
	switch val := v.(type) {
	case map[string]interface{}:
		for k, v2 := range val {
			if k == "type" {
				typ := ""
				if reflect.TypeOf(v2).Kind() == reflect.String {
					typ = v2.(string)
				}
				if reflect.TypeOf(v2).Kind() == reflect.Map {
					// Avro logical type use a map for a type, with an inner type field
					typ = v2.(map[string]interface{})["type"].(string)
				}
				if typ != "" && strings.Contains(typ, ".") && typ != self {
					versions := s.findAllVersionsForSubject(typ)
					for _, version := range versions {
						ref := sr.SchemaReference{
							Subject: typ,
							Name:    typ,
							Version: version,
						}
						(*refs)[ref] = true
					}
				}
			}
			s.findTypeKeys(v2, refs, self)
		}
	case []interface{}:
		for _, item := range val {
			s.findTypeKeys(item, refs, self)
		}
	}
}
