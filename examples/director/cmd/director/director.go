package main

import (
	"context"
	"github.com/cenkalti/backoff"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"open-match.dev/open-match/examples/director/pkg/backendapi"
	"open-match.dev/open-match/pkg/pb"
	"time"
)

func startSendMatchProfile(ctx context.Context, profile *pb.MatchProfile, cfg *viper.Viper, l *log.Entry) {
	for i := 0; i < maxSends || maxSends <= 0; i++ {
		sendLog := l.WithFields(log.Fields{
			"profile": profile.Name,
			"#send":   i,
		})

		sendLog.Infof("Sending profile \"%s\" (attempt #%d/%d)...", profile.Name, i+1, maxSends)
		sendMatchProfile(ctx, profile, cfg, sendLog)

		if i < maxSends - 1 || maxSends <= 0 {
			sendLog.Infof("Sleeping \"%s\"...", profile.Name)
			time.Sleep(sleepBetweenSends)
		}
	}
}

func sendMatchProfile(ctx context.Context, profile *pb.MatchProfile, cfg *viper.Viper, l *log.Entry) {
	var j int
	req := &pb.FetchMatchesRequest{
		Config: &pb.FunctionConfig{
			Host: cfg.GetString("api.functions.hostname"),
			Port: int32(cfg.GetInt("api.functions.grpcport")),
			Type: pb.FunctionConfig_GRPC,
		},
		Profiles: []*pb.MatchProfile{profile},
	}
	err := backendapi.FetchMatches(ctx, req, func(match *pb.Match) (bool, error) {
		matchLog := l.WithField("#recv", j)

		proceed, err := handleMatch(ctx, match, matchLog)
		if err != nil {
			return false, err
		}

		if j++; j >= maxMatchesPerSend && maxMatchesPerSend > 0 {
			matchLog.Debug("Reached max num of match receive attempts, closing stream")
			return false, nil
		}

		return proceed, nil
	})

	if err != nil {
		l.WithError(err).Error(err)
	}
}

func handleMatch(ctx context.Context, match *pb.Match, l *log.Entry) (bool, error) {
	matchLog := l.WithField("match", match.MatchId)

	// Run DGS allocator process
	allocChan := make(chan string, 1)
	go func() {
		defer close(allocChan)

		connString, err := tryAllocate(ctx, match, matchLog)
		if err != nil {
			// Just log error for now
			matchLog.WithError(err).Errorf("Could not allocate a GS for a match %s", match.MatchId)
		} else {
			allocChan <- connString
		}
	}()

	tasks := make(chan *pb.Match, 1)
	tasks <- match

	// Run players assigner process
	go func() {
		connString, ok := <-allocChan
		if !ok {
			return // TODO handle close of channel (meaning allocation failed)
		}

		var first *pb.Match

		for {
			select {
			case <-ctx.Done():
				return

			case task, ok := <-tasks:
				if !ok {
					// TODO handle close of channel
					// May be ok: when partial match got complemented, and no more new matches expected to appear.
					// But also it could be that some error happened.
					return
				}

				tickets := collectTicketIds(task.Tickets)
				matchLog.
					WithField("taskId", task.MatchId).
					WithField("players", tickets).
					WithField("assignment", connString).
					Infof("Assigning %d new players for match %s to DGS at %s", len(tickets), match.MatchId, connString)

				// Distribute connection string to players
				request := &pb.AssignTicketsRequest{TicketIds: tickets, Assignment: &pb.Assignment{Connection:connString}}
				err := backendapi.AssignTickets(ctx, request)
				if err != nil {
					// ???
					// PLAN:
					// - If failed for the first match then delete match & unallocate DGS
					// - If failed for futher match then ignore error? Or just delete that match

					// Just log for now
					matchLog.
						WithField("taskId", task.MatchId).
						WithField("players", tickets).
						WithField("assignment", connString).
						WithError(err).
						Errorf("Error assigning %d new players for match %s to DGS at %s", len(tickets), match.MatchId, connString)
				}
				matchLog.WithField("assignment", connString).WithField("players", tickets).Info("Create assignment")

				// Propagate newly matched rosters to the allocated DGS
				err = allocator.Send(connString, task)
				if err != nil {
					matchLog.
						WithField("taskId", task.MatchId).
						WithField("players", tickets).
						WithField("assignment", connString).
						WithError(err).
						Errorf("Error propagating the details of match %s to DGS at %s", match.MatchId, connString)
				}

				if first == nil {
					first = task
				}
			}
		}
	}()

	return true, nil
}

func collectTicketIds(tickets []*pb.Ticket) (ids []string) {
	for _, t := range tickets {
		ids = append(ids, t.Id)
	}
	return
}

func tryAllocate(ctx context.Context, match *pb.Match, l *log.Entry) (string, error) {
	// Try to allocate a DGS; retry with exponential backoff and jitter
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 2 * time.Second
	b.MaxElapsedTime = 2 * time.Minute
	bt := backoff.NewTicker(backoff.WithContext(b, ctx))

	var connstring string
	for range bt.C {
		connstring, err = allocator.Allocate(match)
		if err != nil {
			l.WithError(err).Error("Allocation attempt failed")
			continue
		}
		bt.Stop()
		break
	}
	if err != nil {
		return "", err
	}
	return connstring, nil
}
