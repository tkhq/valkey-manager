package cluster

import (
	"context"
	"log/slog"

	v1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/tools/cache"
)

type updateHandler struct {
	updater func(*v1.StatefulSet)
}

func (h *updateHandler) OnAdd(obj any, isInInitialList bool) {
	ss, ok := obj.(*v1.StatefulSet)
	if !ok {
		slog.Error("received a non-StatefulSet object")

		return
	}

	h.updater(ss)
}

func (h *updateHandler) OnUpdate(oldObj, newObj any) {
	// NB: we should be isolated to only our own statefulset, at this point, due to the label selector.
	// However, if the label selector is not sufficiently limited, this could result in undefined behaviour.
	oldSS, ok := oldObj.(*v1.StatefulSet)
	if !ok {
		slog.Error("failed to type assert old object, after an update to the StatefulSet was received")

		return
	}

	newSS, ok := newObj.(*v1.StatefulSet)
	if !ok {
		slog.Error("failed to type assert new object, after an update to the StatefulSet was received")

		return
	}

	if *oldSS.Spec.Replicas == *newSS.Spec.Replicas {
		// we only care about changes in replica counts
		return
	}

	h.updater(newSS)
}

func (h *updateHandler) OnDelete(_ any) {
	// Nothing we can do
}

func UpdateHandler(ctx context.Context, ourIndex int) cache.ResourceEventHandler {
	return &updateHandler{
		updater: func(ss *v1.StatefulSet) {
			if err := Configure(ctx, ss, ourIndex); err != nil {
				slog.Error("failed to reconfigure cluster after replica count change", slog.String("error", err.Error()))
			}
		},
	}
}
