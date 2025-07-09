# Valkey Manager

Valkey manager is a sidecar daemon which runs alongside a valkey instance in Kubernetes.

Its job is to watch the StatefulSet to which it is a part and configure the valkey cluster over time, as it changes.
Primarily, this means resharding and re-replicating as the scale changes.
