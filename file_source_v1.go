package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/PaesslerAG/jsonpath"
	"github.com/twmb/franz-go/pkg/sr"
	"math"
	"os"
)

type FileSourceV1 struct {
	Filename string
}

func (f *FileSourceV1) GetState() (*State, error) {

	subjectSchemas := make([]sr.SubjectSchema, 0)
	compatibilityResults := make([]sr.CompatibilityResult, 0)
	softDeletions := make([]sr.SubjectVersion, 0)

	// Open the file
	file, err := os.Open(f.Filename)
	if err != nil {
		return nil, fmt.Errorf("unable to open file: %w", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	// Create a scanner
	scanner := bufio.NewScanner(file)
	lineNumber := 1
	// Read and print lines
	for scanner.Scan() {
		line := scanner.Text()
		//data := make(map[string]interface{})
		v := interface{}(nil)
		err = json.Unmarshal([]byte(line), &v)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshall json at line %v: %w", lineNumber, err)
		}

		keytype, err := jsonpath.Get("$.key.keytype", v)
		if err != nil {
			return nil, fmt.Errorf("unable to find key.keytype at line %v: %w", lineNumber, err)
		}

		if keytype == "CONFIG" {
			subject, err := jsonpath.Get("$.key.subject", v)
			if err != nil {
				if !(fmt.Sprintf("%v", err) == "unknown key subject") {
					return nil, fmt.Errorf("unable to find subject in key at line %v: %w", lineNumber, err)
				}
			} else {
				level, err := jsonpath.Get("$.value.compatibilityLevel", v)
				if err != nil {
					return nil, fmt.Errorf("unable to find compatibility level at line %v: %w", lineNumber, err)
				}
				var compatibilityLevel sr.CompatibilityLevel
				err = compatibilityLevel.UnmarshalText([]byte(level.(string)))
				if err != nil {
					return nil, fmt.Errorf("unable to unmarshall compatibility level at line %v: %w", lineNumber, err)
				}
				compatibilityResults = append(compatibilityResults, sr.CompatibilityResult{Subject: subject.(string), Level: compatibilityLevel})
			}
		}
		if keytype == "SCHEMA" {
			subject, err := jsonpath.Get("$.key.subject", v)
			if err != nil {
				return nil, fmt.Errorf("unable to find key.subject at line %v: %w", lineNumber, err)
			}
			version, err := jsonpath.Get("$.key.version", v)
			if err != nil {
				return nil, fmt.Errorf("unable to find key.version at line %v: %w", lineNumber, err)
			}
			id, err := jsonpath.Get("$.value.id", v)
			if err != nil {
				return nil, fmt.Errorf("unable to find value.id at line %v: %w", lineNumber, err)
			}
			schemaTypeText, err := jsonpath.Get("$.value.schemaType", v)
			if err != nil {
				return nil, fmt.Errorf("unable to find value.schemaType at line %v: %w", lineNumber, err)
			}
			schema, err := jsonpath.Get("$.value.schema", v)
			if err != nil {
				return nil, fmt.Errorf("unable to find value.schema at line %v: %w", lineNumber, err)
			}
			deleted, err := jsonpath.Get("$.value.deleted", v)
			if err != nil {
				return nil, fmt.Errorf("unable to find value.deleted at line %v: %w", lineNumber, err)
			}
			if deleted.(bool) {
				softDeletions = append(softDeletions, sr.SubjectVersion{
					Subject: subject.(string),
					Version: int(math.Round(version.(float64))),
				})
			}
			schemaReferences := make([]sr.SchemaReference, 0)
			references, err := jsonpath.Get("$.value.references", v)
			if err == nil {
				for _, reference := range references.([]interface{}) {
					m := reference.(map[string]interface{})
					ref := sr.SchemaReference{
						Name:    m["name"].(string),
						Subject: m["subject"].(string),
						Version: int(math.Round(m["version"].(float64))),
					}
					schemaReferences = append(schemaReferences, ref)
				}
			}

			var schemaType sr.SchemaType
			err = schemaType.UnmarshalText([]byte(schemaTypeText.(string)))
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshall schemaType at line %v: %w", lineNumber, err)
			}

			// To match what we get from REST
			if len(schemaReferences) == 0 {
				schemaReferences = nil
			}

			subjectSchema := sr.SubjectSchema{
				Subject: subject.(string),
				Version: int(math.Round(version.(float64))),
				ID:      int(math.Round(id.(float64))),
				Schema:  sr.Schema{Schema: schema.(string), Type: schemaType, References: schemaReferences},
			}

			subjectSchemas = append(subjectSchemas, subjectSchema)
		}
	}
	// Check for errors
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner resulted in an error: %w", err)
	}

	var result State
	result.SubjectSchemas = subjectSchemas
	result.CompatibilityResults = compatibilityResults
	result.SoftDeletions = softDeletions

	return &result, nil
}
