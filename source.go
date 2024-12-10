package main

type Source interface {
	GetState() (*State, error)
}
