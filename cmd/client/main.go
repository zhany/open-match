// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"open-match.dev/open-match/internal/config"
	"open-match.dev/open-match/internal/logging"
	"open-match.dev/open-match/internal/rpc"
	"open-match.dev/open-match/pkg/pb"
	"open-match.dev/open-match/pkg/structs"
	"time"
)

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":       "openmatch",
		"component": "examples.client",
	})
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	cfg, err := config.Read()
	if err != nil {
		logger.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatalf("cannot read configuration.")
	}
	logging.ConfigureLogging(cfg)

	run(context.Background(), cfg, "zhany-client-" + RandStringBytes(6))
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func run(ctx context.Context, cfg config.View, name string) {
	defer func() {
		r := recover()
		if r != nil {
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("pkg: %v", r)
			}

			logger.Fatalf("Encountered error: %s", err.Error())
		}
	}()

	logger.Info("Main Menu")
	logger.Info("Connecting to Open Match frontend")
	logger.Infof("api.frontend.hostname: %s", cfg.GetString("api.frontend.hostname"))
	logger.Infof("api.frontend.grpcport: %s", cfg.GetString("api.frontend.grpcport"))
	conn, err := rpc.GRPCClientFromConfig(cfg, "api.frontend")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	fe := pb.NewFrontendClient(conn)

	logger.Info("Creating Open Match Ticket")

	var ticketId string
	{
		req := &pb.CreateTicketRequest{
			Ticket: &pb.Ticket{
				Properties: structs.Struct{
					"name":      structs.String(name),
					"mode.demo": structs.Number(1),
				}.S(),
			},
		}

		resp, err := fe.CreateTicket(ctx, req)
		if err != nil {
			panic(err)
		}
		ticketId = resp.Ticket.Id
	}

	logger.Infof("Waiting match with ticket Id %s", ticketId)

	var assignment *pb.Assignment
	{
		req := &pb.GetAssignmentsRequest{
			TicketId: ticketId,
		}

		stream, err := fe.GetAssignments(ctx, req)
		for assignment.GetConnection() == "" {
			resp, err := stream.Recv()
			if err != nil {
				// For now we don't expect to get EOF, so that's still an error worthy of panic.
				panic(err)
			}

			assignment = resp.Assignment
		}

		err = stream.CloseSend()
		if err != nil {
			panic(err)
		}
	}

	logger.Infof("Assignment found: %s", assignment.Connection)
}