package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tkhq/valkey-manager/cluster"
	"github.com/tkhq/valkey-manager/manager"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var ourIndex int

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	kc, err := getKubernetesClient()
	if err != nil {
		log.Fatal("failed to create kubernetes client:", err)
	}

	manager.NewManager(kc, loadConfig()).Run(ctx, cluster.UpdateHandler(ctx, ourIndex))

	log.Fatal("valkey manager exited")
}

func loadConfig() *manager.Config {
	cfg := new(manager.Config)

	flag.StringVar(&cfg.Namespace, "namespace", "", "kubernetes namespace in which the manager and managed valkey run")
	flag.IntVar(&ourIndex, "index", -1, "index number of this StatefulSet member")
	flag.StringVar(&cfg.LabelSelector, "label", "", "the label selector by which we may find our StatefulSet")

	flag.Parse()

	if cfg.Namespace == "" {
		log.Fatalln("please configure a namespace with the '-namespace' flag")
	}

	if ourIndex < 0 {
		log.Fatalln("please configure an instance index with the '-index' flag")
	}

	return cfg
}

func getKubernetesClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create in-cluster kubernetes config: %w", err)
	}

	return kubernetes.NewForConfig(cfg)
}
