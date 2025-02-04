package main

type ValidateMetadataProcess struct{}

func (f ValidateMetadataProcess) Process(state *State) (*State, error) {

	for _, schema := range state.SubjectSchemas {
		if schema.SchemaMetadata != nil {
			panic(0)
		}
	}
	return state, nil
}
