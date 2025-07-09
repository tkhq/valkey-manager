package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	v1 "k8s.io/api/apps/v1"
)

const (
	PingCheckInterval = time.Second

	// TotalSlotCount is the total number of sharding slots in valkey.
	TotalSlotCount = 16384

	NodeNamePrefix = "valkey-"
	ValkeyPort     = 6379
)

func Configure(ctx context.Context, ss *v1.StatefulSet, ourIndex int) error {
	if ss == nil || ss.Spec.Replicas == nil {
		return fmt.Errorf("failed to locate replica count; cannot configure cluster")
	}

	rc := redis.NewClient(nil)

	if err := WaitPing(ctx, rc); err != nil {
		return fmt.Errorf("failed to wait for local redis ping to succeed: %w", err)
	}

	slog.Info("local redis is alive")

	primaryCount, _ := primariesAndReplicas(int(*ss.Spec.Replicas))

	if err := EnsureClusterInitialized(ctx, rc, ourIndex, primaryCount); err != nil {
		return fmt.Errorf("failed to ensure cluster is initialized: %w", err)
	}

	return nil
}

func primariesAndReplicas(totalCount int) (primaries, replicas int) {
	primaries = (totalCount + 1) / 2

	replicas = (totalCount - primaries) / primaries

	return
}

func WaitPing(ctx context.Context, rc redis.UniversalClient) error {
	for {
		if result := rc.Ping(ctx).String(); result == "PONG" {
			slog.Debug("local redis is ready")

			return nil
		}

		slog.Debug("waiting for local redis to become ready", slog.Duration("wait", PingCheckInterval))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(PingCheckInterval):
		}
	}
}

func EnsureClusterInitialized(ctx context.Context, rc redis.UniversalClient, ourIndex int, primaryCount int) error {
	switch {
	case ourIndex < primaryCount:
		slog.Info("configuring ourselves as a primary node", slog.Int64("index", int64(ourIndex)))

		return ConfigurePrimaryNode(ctx, rc, ourIndex, primaryCount)
	default:
		slog.Info("configuring ourselves as a replica node", slog.Int64("index", int64(ourIndex)))

		return ConfigureReplicaNode(ctx, rc, ourIndex, primaryCount)
	}
}

func ConfigurePrimaryNode(ctx context.Context, rc redis.UniversalClient, ourIndex int, primaryCount int) error {
	log := slog.With(slog.Int64("index", int64(ourIndex)))

	clusterInfo, err := InfoFromString(rc.ClusterInfo(ctx).String())
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

		if err := rc.ClusterAddSlotsRange(ctx, firstSlot, lastSlot).Err(); err != nil {
			return fmt.Errorf("failed to set slot range (%d - %d) on node %d: %w", firstSlot, lastSlot, ourIndex, err)
		}
	}

	for peerIndex := range primaryCount {
		if peerIndex == ourIndex {
			continue
		}

		log.Info("introducting ourselves to peer", slog.Int64("peer_index", int64(peerIndex)))

		if err := rc.ClusterMeet(ctx, nodeName(peerIndex), strconv.FormatInt(ValkeyPort, 10)).Err(); err != nil {
			log.Warn("failed to introduce peer",
				slog.Int64("peer_index", int64(peerIndex)),
				slog.String("error", err.Error()),
			)

			continue
		}
	}

	return nil
}

func ConfigureReplicaNode(ctx context.Context, rc redis.UniversalClient, ourIndex int, primaryCount int) error {
	ourPrimary := ourIndex % primaryCount

	ourPrimaryAddr := net.JoinHostPort(nodeName(ourPrimary), strconv.FormatInt(ValkeyPort, 10))

	primaryClient := redis.NewClient(&redis.Options{
		Addr: ourPrimaryAddr,
	})

	if err := WaitPing(ctx, primaryClient); err != nil {
		return fmt.Errorf("failed to wait for our primary %q to come alive: %w", ourPrimaryAddr, err)
	}

	slog.Info("configuring our valkey instance as a replica",
		slog.Int64("index", int64(ourIndex)),
		slog.String("primary", ourPrimaryAddr),
	)

	return rc.ClusterReplicate(ctx, ourPrimaryAddr).Err()
}

func nodeName(index int) string {
	return NodeNamePrefix + strconv.FormatInt(int64(index), 10)
}

func slotSize(primaryCount int) int {
	return TotalSlotCount / int(primaryCount)
}
