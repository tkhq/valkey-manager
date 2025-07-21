package manager

import (
	"context"
	"net/http"
	"time"

	"github.com/tkhq/valkey-manager/cluster"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	v1 "k8s.io/client-go/informers/apps/v1"
	"k8s.io/client-go/kubernetes"
)

var DefaultResync = time.Minute

type Config struct {
	// Debug enables debug logging.
	Debug bool `env:"DEBUG"`

	// Namespace is the kubernetes namespace in which valkey is running.
	Namespace string `env:"NAMESPACE, required"`

	// LabelSelectoris a kubernetes label selector which can uniquely select the StatefulSet in which valkey is running.
	// This is only needed if there is more than one StatefulSet in this namespace.
	LabelSelector string `env:"LABEL_SELECTOR"`

	// Index is the pod index under which this Pod is running.
	Index uint32 `env:"INDEX, required"`

	// DefaultResync is the deadline interval for resync of data from the kubernetes API server.
	// If not specified, a reasonable default will be used.
	DefaultResync time.Duration `env:"DEFAULT_RESYNC"`

	// ListenAddr is the host:port on which the HTTP service (for health) should listen.
	ListenAddr string `env:"LISTEN_ADDR, default=:8087"`
}

// Manager implements a valkey manager, which configures the runtime cluster dynamics of a Valkey instance.
// It is expected that the Valkey instance be a member of a StatefulSet.
type Manager struct {
	informer   v1.StatefulSetInformer
	listenAddr string
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
		informer:   informer,
		listenAddr: cfg.ListenAddr,
	}
}

func (m *Manager) Run(ctx context.Context, handler cluster.ResourceHandler) {
	go m.runHealthService(handler)

	m.informer.Informer().AddEventHandler(handler)

	m.informer.Informer().RunWithContext(ctx)
}

func (m *Manager) runHealthService(h cluster.ResourceHandler) {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if h == nil || m == nil {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if h.ClusterConfigured() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	http.ListenAndServe(m.listenAddr, mux)
}
