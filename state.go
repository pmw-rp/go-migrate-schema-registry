package main

import (
	"fmt"
	"github.com/twmb/franz-go/pkg/sr"
	"slices"
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

func (s *State) sort() {
	slices.SortFunc(s.SubjectSchemas, func(a, b sr.SubjectSchema) int {
		comparison := a.ID - b.ID
		if comparison != 0 {
			return comparison
		}
		comparison = a.Version - b.Version
		return comparison
	})
}
