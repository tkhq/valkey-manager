package cluster

import (
	"bufio"
	"bytes"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

// State is the state of the valkey cluster, as reported by its `CLUSTER INFO` command.
type State string

const (
	StateInfoKey = "cluster_state"

	StateUnknown = ""
	StateOK      = "ok"
	StateFail    = "fail"

	EpochInfoKey          = "cluster_current_epoch"
	KnownNodeCountInfoKey = "cluster_known_nodes"
	SizeInfoKey           = "cluster_size"
	SlotsAssignedInfoKey  = "cluster_slots_assigned"
	LocalEpochInfoKey     = "cluster_current_epoch"
)

// Info represents the state of the cluster, as seen from a given node.
type Info map[string]string

func InfoFromString(in string) (Info, error) {
	i := Info{}

	b := bytes.NewReader([]byte(in))

	scanner := bufio.NewScanner(b)

	for scanner.Scan() {
		pieces := strings.Split(scanner.Text(), ":")
		if len(pieces) != 2 {
			slog.Warn("unhandled cluster info line", slog.String("text", scanner.Text()))

			continue
		}

		i[pieces[0]] = pieces[1]
	}

	if len(i) < 1 {
		return nil, fmt.Errorf("no cluster info found")
	}

	return i, nil
}

func (ci Info) ClusterEpoch() int32 {
	return ci.getInt32(EpochInfoKey)
}

func (ci Info) LocalEpoch() int32 {
	return ci.getInt32(LocalEpochInfoKey)
}

// Size returns the number of primary nodes in the cluster.
func (ci Info) Size() int32 {
	return ci.getInt32(SizeInfoKey)
}

// SlotsAssigned indicates the number of slots assigned to any node int the cluster.
func (ci Info) SlotsAssigned() int32 {
	return ci.getInt32(SlotsAssignedInfoKey)
}

func (ci Info) KnownNodeCount() int32 {
	return ci.getInt32(KnownNodeCountInfoKey)
}

func (ci Info) State() State {
	if ci == nil {
		slog.Error("no cluster info found")

		return StateUnknown
	}

	stateString, ok := ci[StateInfoKey]
	if !ok {
		slog.Error("no cluster state found in cluster info")

		return StateUnknown
	}

	return State(stateString)
}

func (ci Info) getInt32(keyName string) int32 {
	if ci == nil {
		slog.Error("no cluster info found")

		return -1
	}

	valString, ok := ci[keyName]
	if !ok {
		slog.Error(fmt.Sprintf("no %s found in cluster info", keyName))

		return -1
	}

	val, err := strconv.ParseInt(valString, 10, 32)
	if err != nil {
		slog.Error(fmt.Sprintf("invalid %s: %s", keyName, valString))

		return -1
	}

	return int32(val)
}
