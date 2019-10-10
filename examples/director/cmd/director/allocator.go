package main

import (
	"open-match.dev/open-match/pkg/pb"
)

// Allocator is the interface that allocates a game server for a match object
type Allocator interface {
	Allocate(*pb.Match) (string, error)
	Send(string, *pb.Match) error
	UnAllocate(string) error
}
