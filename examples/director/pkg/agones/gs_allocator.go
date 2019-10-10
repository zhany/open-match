package agones

import (
	"encoding/json"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/types"
	"strconv"
	"strings"

	agonesv1 "agones.dev/agones/pkg/apis/agones/v1"
	allocationv1 "agones.dev/agones/pkg/apis/allocation/v1"
	"agones.dev/agones/pkg/client/clientset/versioned"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"open-match.dev/open-match/pkg/pb"
)

// GameServerAllocator allocates game servers in Agones fleet
type GameServerAllocator struct {
	agonesClient *versioned.Clientset

	namespace    string
	fleetName    string
	generateName string

	l *logrus.Entry
}

// NewAllocator creates new GameServerAllocator with in cluster k8s config
func NewGameServerAllocator(namespace, fleetName, generateName string, l *logrus.Entry) (*GameServerAllocator, error) {
	agonesClient, err := getAgonesClient()
	if err != nil {
		return nil, errors.New("Could not create Agones game server allocator: " + err.Error())
	}

	a := &GameServerAllocator{
		agonesClient: agonesClient,

		namespace:    namespace,
		fleetName:    fleetName,
		generateName: generateName,

		l: l.WithFields(logrus.Fields{
			"source":       "agones",
			"namespace":    namespace,
			"fleetname":    fleetName,
			"generatename": generateName,
		}),
	}
	return a, nil
}

// Set up our client which we will use to call the API
func getAgonesClient() (*versioned.Clientset, error) {
	// Create the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.New("Could not create in cluster config: " + err.Error())
	}

	// Access to the Agones resources through the Agones Clientset
	agonesClient, err := versioned.NewForConfig(config)
	if err != nil {
		return nil, errors.New("Could not create the agones api clientset: " + err.Error())
	}
	return agonesClient, nil
}

// Return the number of ready game servers available to this fleet for allocation
func (a *GameServerAllocator) checkReadyReplicas() int32 {
	// Get a FleetInterface for this namespace
	fleetInterface := a.agonesClient.AgonesV1().Fleets(a.namespace)
	// Get our fleet
	fleet, err := fleetInterface.Get(a.fleetName, metav1.GetOptions{})
	if err != nil {
		a.l.WithError(err).Info("Get fleet failed")
	}

	return fleet.Status.ReadyReplicas
}

// Allocate allocates a game server in a fleet, distributes match object details to it,
// and returns a connection string or error
func (a *GameServerAllocator) Allocate(match *pb.Match) (string, error) {
	// Log the values used in the allocation
	a.l.WithField("namespace", a.namespace).Info("namespace for gsa")
	a.l.WithField("fleetname", a.fleetName).Info("fleetname for gsa")

	// Find out how many ready replicas the fleet has - we need at least one
	readyReplicas := a.checkReadyReplicas()
	// Log and return an error if there are no ready replicas
	if readyReplicas < 1 {
		a.l.WithField("fleetname", a.fleetName).Info("Insufficient ready replicas, cannot create fleet allocation")
		return "", errors.New("insufficient ready replicas, cannot create fleet allocation")
	}

	// Get a AllocationInterface for this namespace
	allocationInterface := a.agonesClient.AllocationV1().GameServerAllocations(a.namespace)

	// Define the allocation using the constants set earlier
	gsa := &allocationv1.GameServerAllocation{
		Spec: allocationv1.GameServerAllocationSpec{
			Required: metav1.LabelSelector{MatchLabels: map[string]string{agonesv1.FleetNameLabel: a.fleetName}},
		}}

	// Create a new allocation
	gsa, err := allocationInterface.Create(gsa)
	if err != nil {
		// Log and return the error if the call to Create fails
		a.l.WithError(err).Info("Failed to create allocation")
		return "", errors.New("failed to create allocation")
	}

	// Log the GameServer.Status of the new allocation, then return those values
	a.l.Info("New GameServer allocated: ", gsa.Status.State)

	a.l.Infof("allocation status: %v", gsa)
	connString := fmt.Sprintf("%s:%d", gsa.Status.Address, gsa.Status.Ports[0].Port)

	a.l.WithFields(logrus.Fields{
		"fleetAllocation": gsa.Name,
		"gameServer":      gsa.Status.GameServerName,
		"connString":      connString,
	}).Info("GameServer allocated")

	return connString, nil
}

func (a *GameServerAllocator) getAllocationMeta(match *pb.Match) (labels, annotations map[string]string) {
	labels = map[string]string{
		"openmatch/match": match.MatchId,
	}

	annotations = map[string]string{}

	//if profile, err := json.Marshal(match.Rosters); err == nil {
	//	annotations["openmatch/profiles"] = string(pools)
	//} else {
	//	a.l.WithField("match", match.Id).
	//		WithError(err).
	//		Error("Could not marshal MatchObject.Pools to attach to FleetAllocation metadata")
	//}

	// Propagate the rosters using the distinct method Send()

	return
}

// Send updates the metadata of allocated GS at given connString
func (a *GameServerAllocator) Send(connString string, match *pb.Match) error {
	gs, err := a.findGameServer(connString)
	if err != nil {
		return errors.New("could not find game server: " + err.Error())
	}

	gs0 := gs.DeepCopy()
	a.copyNewlyFilledRosters(gs, match)
	patch, err := gs0.Patch(gs)
	if err != nil {
		return errors.New("could not compute JSON patch to update the game server metadata: " + err.Error())
	}

	gsi := a.agonesClient.AgonesV1().GameServers(a.namespace)
	_, err = gsi.Patch(gs0.Name, types.JSONPatchType, patch)
	if err != nil {
		return errors.New("could not update game server: " + err.Error())
	}

	a.l.WithField("connString", connString).
		WithField("gameServer", gs.Name).
		WithField("match.id", match.MatchId).
		WithField("patch", string(patch)).
		Info("Data from new MatchObject was sent to game server")
	return nil
}

func (a *GameServerAllocator) copyNewlyFilledRosters(gs *agonesv1.GameServer, newMatch *pb.Match) {
	var rosters = newMatch.Rosters

	//if curJ, ok := gs.Annotations["openmatch/rosters"]; !ok {
	//	// If it's missing then most probably it was never set
	//	rosters = newMatch.Rosters
	//
	//} else {
	//	if err := json.Unmarshal([]byte(curJ), &rosters); err != nil {
	//		a.l.WithError(err).
	//			WithField("gs.name", gs.Name).
	//			WithField("openmatch/rosters", gs.Annotations["openmatch/rosters"]).
	//			Error("Could not unmarshal the value of rosters annotation")
	//		return
	//	}
	//
	//	// Iterate over newly filled rosters and copy players into empty slots of current rosters
	//	for _, fromRoster := range newMatch.Rosters {
	//		// Find matching current roster
	//		var roster *pb.Roster
	//		for _, intoRoster := range rosters {
	//			if intoRoster.Name == fromRoster.Name {
	//				roster = intoRoster
	//				break
	//			}
	//		}
	//		if roster != nil {
	//			// Copy player IDs into empty slots of matching current roster
	//			for _, p := range fromRoster.TicketIds {
	//				// Find matching empty slot
	//				var room *pb.Ticket
	//				for _, slot := range roster.TicketIds {
	//					if slot == p {
	//						room = slot
	//						break
	//					}
	//				}
	//				if room != nil {
	//					room.Id = p.Id
	//				}
	//			}
	//		}
	//	}
	//
	//}

	newJ, err := json.Marshal(rosters)
	if err != nil {
		a.l.WithError(err).
			WithField("gs.name", gs.Name).
			WithField("open-match/rosters", gs.Annotations["open-match/rosters"]).
			WithField("newMatch", newMatch).
			WithField("rosters", rosters).
			Error("Could not marshal the updated rosters to JSON")
		return
	}

	gs.Annotations["open-match/rosters"] = string(newJ)
}

// UnAllocate finds and deletes the allocated game server matching the specified connection string
func (a *GameServerAllocator) UnAllocate(connstring string) error {
	gs, err := a.findGameServer(connstring)
	if err != nil {
		return errors.New("could not find game server: " + err.Error())
	}
	if gs == nil {
		return errors.New("found no game servers matching the connection string")
	}

	fields := logrus.Fields{
		"connstring": connstring,
		"gameserver": gs.Name,
	}

	gsi := a.agonesClient.AgonesV1().GameServers(a.namespace)
	err = gsi.Delete(gs.Name, nil)
	if err != nil {
		msg := "failed to delete game server"
		a.l.WithFields(fields).WithError(err).Error(msg)
		return errors.New(msg + ": " + err.Error())
	}
	a.l.WithFields(fields).Info("GameServer deleted")

	return nil
}

func (a *GameServerAllocator) findGameServer(connString string) (*agonesv1.GameServer, error) {
	var ip, port string
	if parts := strings.Split(connString, ":"); len(parts) != 2 {
		return nil, errors.New("unable to parse connection string: expecting format \"<IP>:<PORT>\"")
	} else {
		ip, port = parts[0], parts[1]
	}

	gsi := a.agonesClient.AgonesV1().GameServers(a.namespace)

	gsl, err := gsi.List(metav1.ListOptions{})
	if err != nil {
		return nil, errors.New("failed to get game servers list: " + err.Error())
	}

	for _, gs := range gsl.Items {
		if /*gs.Status.State == "Allocated" &&*/ gs.Status.Address == ip {
			for _, p := range gs.Status.Ports {
				if strconv.Itoa(int(p.Port)) == port {
					return &gs, nil
				}
			}
		}
	}
	return nil, nil
}
