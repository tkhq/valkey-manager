package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sethvargo/go-envconfig"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/tkhq/valkey-manager/cluster"
	"github.com/tkhq/valkey-manager/manager"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := loadConfig(ctx)
	if err != nil {
		log.Fatal("failed to load configuration: ", err)
	}

	if cfg.Debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	kc, err := getKubernetesClient()
	if err != nil {
		log.Fatal("failed to create kubernetes client: ", err)
	}

	manager.NewManager(kc, cfg).Run(ctx, cluster.UpdateHandler(ctx, cfg.Index))

	log.Fatal("valkey manager exited")
}

func loadConfig(ctx context.Context) (*manager.Config, error) {
	cfg := new(manager.Config)

	if err := envconfig.Process(ctx, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func getKubernetesClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create in-cluster kubernetes config: %w", err)
	}

	return kubernetes.NewForConfig(cfg)
}
