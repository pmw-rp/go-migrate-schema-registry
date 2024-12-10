package main

import (
	"fmt"
	"github.com/twmb/franz-go/pkg/sr"
	"reflect"
)

func validate(a *State, b *State) {

	// Build Lookup Tables

	aSubjectSchemas := make(map[sr.SubjectVersion]*sr.SubjectSchema)
	for _, subjectSchema := range a.SubjectSchemas {
		ref := getReference(subjectSchema)
		aSubjectSchemas[ref] = &subjectSchema
	}

	aCompatibilityResults := make(map[string]*sr.CompatibilityResult)
	for _, compatibilityResult := range a.CompatibilityResults {
		aCompatibilityResults[compatibilityResult.Subject] = &compatibilityResult
	}

	aSoftDeletions := make(map[sr.SubjectVersion]bool)
	for _, reference := range a.SoftDeletions {
		aSoftDeletions[reference] = true
	}

	bSubjectSchemas := make(map[sr.SubjectVersion]*sr.SubjectSchema)
	for _, subjectSchema := range b.SubjectSchemas {
		ref := getReference(subjectSchema)
		bSubjectSchemas[ref] = &subjectSchema
	}

	bCompatibilityResults := make(map[string]*sr.CompatibilityResult)
	for _, compatibilityResult := range b.CompatibilityResults {
		bCompatibilityResults[compatibilityResult.Subject] = &compatibilityResult
	}

	bSoftDeletions := make(map[sr.SubjectVersion]bool)
	for _, reference := range b.SoftDeletions {
		bSoftDeletions[reference] = true
	}

	// Compare schemaSubjects

	for _, aSubjectSchema := range a.SubjectSchemas {
		bSubjectSchema, ok := bSubjectSchemas[getReference(aSubjectSchema)]
		if !ok {
			panic(fmt.Errorf("subject %v version %v not found in right", aSubjectSchema.Subject, aSubjectSchema.Version))
		} else {
			if aSubjectSchema.ID != bSubjectSchema.ID {
				panic(fmt.Errorf("subject %v version %v schema IDs don't match: %v vs %v", aSubjectSchema.Subject, aSubjectSchema.Version, aSubjectSchema.ID, bSubjectSchema.ID))
			}
			if aSubjectSchema.ID != bSubjectSchema.ID {
				panic(fmt.Errorf("subject %v version %v schemas don't match: %v vs %v", aSubjectSchema.Subject, aSubjectSchema.Version, aSubjectSchema.Schema, bSubjectSchema.Schema))
			}
			if aSubjectSchema.Type != bSubjectSchema.Type {
				panic(fmt.Errorf("subject %v version %v schema types don't match: %v vs %v", aSubjectSchema.Subject, aSubjectSchema.Version, aSubjectSchema.Type, bSubjectSchema.Type))
			}
			if !reflect.DeepEqual(aSubjectSchema.References, bSubjectSchema.References) {
				panic(fmt.Errorf("subject %v version %v references don't match: %v vs %v", aSubjectSchema.Subject, aSubjectSchema.Version, aSubjectSchema.References, bSubjectSchema.References))
			}
		}
	}

	for _, bSubjectSchema := range b.SubjectSchemas {
		aSubjectSchema, ok := aSubjectSchemas[getReference(bSubjectSchema)]
		if !ok {
			panic(fmt.Errorf("subject %v version %v not found in left", bSubjectSchema.Subject, bSubjectSchema.Version))
		} else {
			if aSubjectSchema.ID != bSubjectSchema.ID {
				panic(fmt.Errorf("subject %v version %v schema IDs don't match: %v vs %v", aSubjectSchema.Subject, aSubjectSchema.Version, aSubjectSchema.ID, bSubjectSchema.ID))
			}
			if aSubjectSchema.ID != bSubjectSchema.ID {
				panic(fmt.Errorf("subject %v version %v schemas don't match: %v vs %v", aSubjectSchema.Subject, aSubjectSchema.Version, aSubjectSchema.Schema, bSubjectSchema.Schema))
			}
			if aSubjectSchema.Type != bSubjectSchema.Type {
				panic(fmt.Errorf("subject %v version %v schema types don't match: %v vs %v", aSubjectSchema.Subject, aSubjectSchema.Version, aSubjectSchema.Type, bSubjectSchema.Type))
			}
			if !reflect.DeepEqual(aSubjectSchema.References, bSubjectSchema.References) {
				panic(fmt.Errorf("subject %v version %v references don't match: %v vs %v", aSubjectSchema.Subject, aSubjectSchema.Version, aSubjectSchema.References, bSubjectSchema.References))
			}
		}
	}

}
