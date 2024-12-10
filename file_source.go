package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

type FileSource struct {
	Filename string `koanf:"filename"`
}

func (f *FileSource) GetState() (*State, error) {
	var state State
	data, err := os.ReadFile(f.Filename)
	if err != nil {
		return nil, fmt.Errorf("unable to read %v: %w", f.Filename, err)
	}
	err = yaml.Unmarshal(data, &state)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshall yaml from %v: %w", f.Filename, err)
	}
	return &state, nil
}
