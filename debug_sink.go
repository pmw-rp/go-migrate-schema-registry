package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
)

type DebugSink struct {
}

func (r *DebugSink) PutState(state *State) error {

	data, err := yaml.Marshal(state)
	if err != nil {
		return err
	}
	fmt.Println(string(data))

	return nil
}
