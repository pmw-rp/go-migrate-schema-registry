package main

type Sink interface {
	PutState(*State) error
}
