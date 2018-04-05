package main

import "fmt"

// Query holds Timescale SQL query, typically decoded from the program's
// input.
type Query struct {
	HumanLabel       []byte
	HumanDescription []byte
	QuerySQL         []byte
	ID               int64
}

// String produces a debug-ready description of a Query.
func (q *Query) String() string {
	return fmt.Sprintf("ID: %d, HumanLabel: %s, HumanDescription: %s, Query: %s", q.ID, q.HumanLabel, q.HumanDescription, q.QuerySQL)
}
