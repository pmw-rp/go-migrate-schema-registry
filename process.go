package main

type Process interface {
	Process(*State) (*State, error)
}
