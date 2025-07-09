package manager

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	v1 "k8s.io/client-go/informers/apps/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

var DefaultResync = time.Minute

type Config struct {
	Namespace     string
	LabelSelector string
	DefaultResync time.Duration
}

// Manager implements a valkey manager, which configures the runtime cluster dynamics of a Valkey instance.
// It is expected that the Valkey instance be a member of a StatefulSet.
type Manager struct {
	informer v1.StatefulSetInformer
}

func NewManager(kc kubernetes.Interface, cfg *Config) *Manager {
	opts := []informers.SharedInformerOption{
		informers.WithNamespace(cfg.Namespace),
	}

	if cfg.LabelSelector != "" {
		opts = append(opts, informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = cfg.LabelSelector
		}))
	}

	if cfg.DefaultResync == 0 {
		cfg.DefaultResync = DefaultResync
	}

	informerFactory := informers.NewSharedInformerFactoryWithOptions(kc, cfg.DefaultResync, opts...)

	informer := informerFactory.Apps().V1().StatefulSets()

	return &Manager{
		informer: informer,
	}
}

func (m *Manager) Run(ctx context.Context, handler cache.ResourceEventHandler) {
	m.informer.Informer().AddEventHandler(handler)

	m.informer.Informer().RunWithContext(ctx)
}
