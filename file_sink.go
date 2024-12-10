package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

type FileSink struct {
	Filename string
}

func (f *FileSink) PutState(state *State) error {

	data, err := yaml.Marshal(state)
	if err != nil {
		return fmt.Errorf("unable to marshall yaml: %w", err)
	}
	err = os.WriteFile(f.Filename, data, 0644)
	if err != nil {
		return fmt.Errorf("unable to write file %v: %w", f.Filename, err)
	}
	return nil
}
