package backendapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"google.golang.org/grpc"

	"open-match.dev/open-match/pkg/pb"
)

// Connect to in-cluster Open-match BackendAPI service
func Connect() (*grpc.ClientConn, pb.BackendClient, error) {
	addrs, err := net.LookupHost("om-backend.open-match")
	if err != nil {
		return nil, nil, errors.New("error creating Backend API client: lookup failed: " + err.Error())
	}

	addr := fmt.Sprintf("%s:50505", addrs[0])

	beAPIConn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, errors.New("error creating Backend API client: failed to connect: " + err.Error())
	}
	beAPI := pb.NewBackendClient(beAPIConn)
	return beAPIConn, beAPI, nil
}

func FetchMatches(ctx context.Context, request *pb.FetchMatchesRequest, fn MatchFunc) error {
	beAPIConn, beAPI, err := Connect()
	if err != nil {
		return err
	}
	defer beAPIConn.Close()

	stream, err := beAPI.FetchMatches(ctx, request)
	if err != nil {
		return errors.New("Error opening matches stream: " + err.Error())
	}

	for {
		match, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			stream.CloseSend()
			return errors.New("Error receiving match: " + err.Error())
		}

		var ok bool
		if ok, err = fn(match.Match); err != nil {
			stream.CloseSend()
			return errors.New("Error processing match: " + err.Error())
		}
		if !ok {
			stream.CloseSend()
			return nil
		}
	}
}

func AssignTickets(ctx context.Context, request *pb.AssignTicketsRequest) error {
	beAPIConn, beAPI, err := Connect()
	if err != nil {
		return err
	}
	defer beAPIConn.Close()

	_, err = beAPI.AssignTickets(ctx, request)
	if err != nil {
		return errors.New("error creating assignments: " + err.Error())
	}
	return nil
}

// MatchFunc is a function that is applied to each item of ListMatches() stream.
// Iteration is stopped and stream is closed if function return false or error.
type MatchFunc func(*pb.Match) (bool, error)
