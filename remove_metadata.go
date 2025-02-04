package main

type RemoveMetadataProcess struct{}

func (f RemoveMetadataProcess) Process(state *State) (*State, error) {
	for i := range state.SubjectSchemas {
		state.SubjectSchemas[i].SchemaMetadata = nil
	}
	return state, nil
}
