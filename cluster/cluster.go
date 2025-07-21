package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"
	v1 "k8s.io/api/apps/v1"
)

const (
	PingCheckInterval = time.Second

	// TotalSlotCount is the total number of sharding slots in valkey.
	TotalSlotCount = 16384

	NodeNamePrefix = "valkey-"
	NodeNameSuffix = ".valkey"

	ValkeyPort = 6379
)

func Configure(ctx context.Context, ss *v1.StatefulSet, ourIndex uint32) error {
	if ss == nil || ss.Spec.Replicas == nil {
		return fmt.Errorf("failed to locate replica count; cannot configure cluster")
	}

	vc, err := WaitPing(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(ValkeyPort)))
	if err != nil {
		return fmt.Errorf("failed to wait for local redis ping to succeed: %w", err)
	}

	slog.Info("local redis is alive")

	primaryCount, _ := primariesAndReplicas(int(*ss.Spec.Replicas))

	if err := EnsureClusterInitialized(ctx, vc, int(ourIndex), primaryCount); err != nil {
		return fmt.Errorf("failed to ensure cluster is initialized: %w", err)
	}

	return nil
}

// primariesAndReplicas splits the total count of instances between primaries and replicas.
// Primaries are enumerated first as half the total, rounded down, and replicas assigned from the remnant.
// We number these to ensure that if there are _any_ replicas, their count is at least the same as the primaries, totalCount
// ensure that each primary has a replica.
func primariesAndReplicas(totalCount int) (primaries, replicas int) {
	primaries = max(1, totalCount/2)

	return primaries, totalCount - primaries
}

func WaitPing(ctx context.Context, addr string) (valkey.Client, error) {
	for {
		vc, err := valkey.NewClient(valkey.ClientOption{
			InitAddress:       []string{addr},
			ForceSingleClient: true,
		})
		if err == nil {
			if err = vc.Do(ctx, vc.B().Ping().Build()).Error(); err == nil {
				slog.Debug("local redis is ready")

				return vc, nil
			}
		}

		slog.Debug("waiting for local redis to become ready",
			slog.Duration("wait", PingCheckInterval),
			slog.String("error", err.Error()),
		)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(PingCheckInterval):
		}
	}
}

func EnsureClusterInitialized(ctx context.Context, vc valkey.Client, ourIndex int, primaryCount int) error {
	switch {
	case ourIndex < primaryCount:
		slog.Info("configuring ourselves as a primary node", slog.Int64("index", int64(ourIndex)))

		return ConfigurePrimaryNode(ctx, vc, ourIndex, primaryCount)
	default:
		slog.Info("configuring ourselves as a replica node", slog.Int64("index", int64(ourIndex)))

		return ConfigureReplicaNode(ctx, vc, ourIndex, primaryCount)
	}
}

func ConfigurePrimaryNode(ctx context.Context, vc valkey.Client, ourIndex int, primaryCount int) error {
	log := slog.With(slog.Int64("index", int64(ourIndex)))

	infoReader, err := vc.Do(ctx, vc.B().ClusterInfo().Build()).AsReader()
	if err != nil {
		return fmt.Errorf("failed to read cluster info: %w", err)
	}

	clusterInfo, err := InfoFromReader(infoReader)
	if err != nil {
		return fmt.Errorf("failed to read cluster info: %w", err)
	}

	// Ensure the epoch is set.
	if clusterInfo.LocalEpoch() > 0 {
		log.Debug("cluster epoch is configured")
	} else {
		log.Info("setting cluster epoch equal to node index + 1", slog.Int64("epoch", int64(ourIndex+1)))
	}

	// Ensure slots are configured.
	if clusterInfo.SlotsAssigned() > 0 {
		log.Debug("slots for this node are already assigned")
	} else {
		firstSlot := int(ourIndex) * slotSize(primaryCount)
		lastSlot := max(TotalSlotCount, firstSlot+slotSize(primaryCount)) - 1

		log.Info("setting cluster shard slots", slog.Int("first", firstSlot), slog.Int("last", lastSlot))

		if err := vc.Do(ctx, vc.B().ClusterAddslotsrange().StartSlotEndSlot().StartSlotEndSlot(int64(firstSlot), int64(lastSlot)).Build()).Error(); err != nil {
			return fmt.Errorf("failed to set slot range (%d - %d) on node %d: %w", firstSlot, lastSlot, ourIndex, err)
		}
	}

	for peerIndex := range primaryCount {
		if peerIndex == ourIndex {
			continue
		}

		log.Info("introducting ourselves to peer", slog.Int64("peer_index", int64(peerIndex)))

		peerIP, err := nodeIP(peerIndex)
		if err != nil {
			log.Warn("no IP available for peer",
				slog.Int64("peer_index", int64(peerIndex)),
				slog.String("error", err.Error()),
			)

			continue
		}

		if err := vc.Do(ctx, vc.B().ClusterMeet().Ip(peerIP.String()).Port(ValkeyPort).Build()).Error(); err != nil {
			log.Warn("failed to introduce peer",
				slog.Int64("peer_index", int64(peerIndex)),
				slog.String("error", err.Error()),
			)

			continue
		}
	}

	return nil
}

func ConfigureReplicaNode(ctx context.Context, vc valkey.Client, ourIndex int, primaryCount int) error {
	ourPrimary := ourIndex % primaryCount

	ourPrimaryIP, err := nodeIP(ourPrimary)
	if err != nil {
		return fmt.Errorf("failed to find IP for our primary: %w", err)
	}

	ourPrimaryAddr := net.JoinHostPort(ourPrimaryIP.String(), strconv.FormatInt(ValkeyPort, 10))

	if _, err := WaitPing(ctx, ourPrimaryAddr); err != nil {
		return fmt.Errorf("failed to wait for our primary %q to come alive: %w", ourPrimaryAddr, err)
	}

	slog.Info("configuring our valkey instance as a replica",
		slog.Int64("index", int64(ourIndex)),
		slog.String("primary", ourPrimaryAddr),
	)

	return vc.Do(ctx, vc.B().ClusterReplicate().NodeId(ourPrimaryAddr).Build()).Error()
}

func nodeName(index int) string {
	return NodeNamePrefix + strconv.FormatInt(int64(index), 10) + NodeNameSuffix
}

func nodeIP(index int) (net.IP, error) {
	ips, err := net.LookupIP(nodeName(index))
	if err != nil {
		return nil, fmt.Errorf("failed to lookup IP for %s: %w", nodeName(index), err)
	}

	if len(ips) < 1 {
		return nil, fmt.Errorf("no IPs found for %s", nodeName(index))
	}

	return ips[0], nil
}

func slotSize(primaryCount int) int {
	return TotalSlotCount / int(primaryCount)
}
