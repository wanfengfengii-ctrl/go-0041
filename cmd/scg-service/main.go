// Command scg-service runs the specimen-custody-graph HTTP service. On startup
// it recovers published state from the snapshot and event log, then serves the
// HTTP API. A snapshot is written on graceful shutdown so the next start is
// fast.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"specimen-custody-graph/coord"
	"specimen-custody-graph/eventstore"
	"specimen-custody-graph/httpapi"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/lims"
	"specimen-custody-graph/recovery"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dataDir := flag.String("data", "./data", "data directory for log and snapshot")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("mkdir data dir: %v", err)
	}
	logPath := filepath.Join(*dataDir, "events.log")
	snapPath := filepath.Join(*dataDir, "snapshot.json")

	syncer := infra.RealSyncer{}
	store, err := eventstore.Open(logPath, syncer)
	if err != nil {
		log.Fatalf("open log: %v", err)
	}
	defer store.Close()

	snaps := eventstore.NewSnapshotStore(snapPath, syncer)

	// recover from snapshot + log
	res, err := recovery.FromSnapshot(store, snaps)
	if err != nil {
		log.Fatalf("recover: %v", err)
	}
	log.Printf("recovered %d families, last_seq=%d (from_snapshot=%v)",
		len(res.Families), res.LastSeq, res.FromSnapshot)

	idgen := infra.NewRandIDGen("scg")
	clock := infra.RealClock{}
	co := coord.New(coord.Options{
		Store:   store,
		Clock:   clock,
		IDGen:   idgen,
		Barrier: infra.NoopBarrier{},
	}, res.Families, res.LastSeq)

	keys := infra.NewMapKeyStore(map[string][]byte{
		"default": []byte("change-me-default-secret"),
	})
	adapter := lims.NewAdapter(co, keys)

	srv := httpapi.New(co, store, adapter, idgen)
	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("listening on %s", *addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down; writing snapshot")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)

	if err := co.Snapshot(snaps); err != nil {
		log.Printf("snapshot on shutdown: %v", err)
	} else {
		log.Printf("snapshot written at seq %d", co.AppliedSeq())
	}
}

// usage prints a short usage message.
func usage() {
	fmt.Fprintln(os.Stderr, "usage: scg-service [-addr :8080] [-data ./data]")
}
