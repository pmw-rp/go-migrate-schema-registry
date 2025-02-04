package main

import "github.com/twmb/franz-go/pkg/sr"

type AddMetadataProcess struct{}

func (f AddMetadataProcess) Process(state *State) (*State, error) {

	for i := range state.SubjectSchemas {
		metadata := buildMetadata()
		state.SubjectSchemas[i].SchemaMetadata = metadata
		//schema.Schema.SchemaMetadata = metadata
		_ = "foo"
	}
	return state, nil
}

func buildMetadata() *sr.SchemaMetadata {
	metadata := sr.SchemaMetadata{
		Tags:       map[string][]string{"foo": {"bar"}},
		Properties: map[string]string{"foo": "bar"},
		Sensitive:  []string{"foo"},
	}
	return &metadata
}
