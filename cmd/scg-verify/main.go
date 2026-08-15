// Command scg-verify is the offline recovery and verification tool. It can:
//
//	scg-verify verify  [-data DIR]   compare snapshot+log digest with full-log digest
//	scg-verify rebuild [-data DIR]   ignore snapshot, rebuild from log, print digest
//	scg-verify snapshot [-data DIR]  rebuild from log and write a fresh snapshot
//
// Exit code is non-zero when verification fails (digest mismatch, corrupt or
// truncated log).
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"specimen-custody-graph/eventstore"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/recovery"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	dataDir := fs.String("data", "./data", "data directory")
	_ = fs.Parse(os.Args[2:])

	logPath := filepath.Join(*dataDir, "events.log")
	snapPath := filepath.Join(*dataDir, "snapshot.json")

	syncer := infra.RealSyncer{}
	store, err := eventstore.Open(logPath, syncer)
	if err != nil {
		log.Fatalf("open log: %v", err)
	}
	defer store.Close()
	snaps := eventstore.NewSnapshotStore(snapPath, syncer)

	switch cmd {
	case "verify":
		logDigest, snapDigest, err := recovery.Verify(store, snaps)
		if err != nil {
			log.Fatalf("verify failed: %v", err)
		}
		if logDigest != snapDigest {
			fmt.Printf("MISMATCH log=%s snapshot=%s\n", logDigest, snapDigest)
			os.Exit(1)
		}
		fmt.Printf("OK digest=%s\n", logDigest)
	case "rebuild":
		res, err := recovery.ForcedRebuild(store)
		if err != nil {
			log.Fatalf("rebuild failed: %v", err)
		}
		fmt.Printf("OK digest=%s families=%d last_seq=%d\n", res.Digest, len(res.Families), res.LastSeq)
	case "snapshot":
		res, err := recovery.ForcedRebuild(store)
		if err != nil {
			log.Fatalf("rebuild failed: %v", err)
		}
		if err := snaps.Write(res.Families, res.LastSeq); err != nil {
			log.Fatalf("write snapshot: %v", err)
		}
		fmt.Printf("OK snapshot written digest=%s last_seq=%d\n", res.Digest, res.LastSeq)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: scg-verify <verify|rebuild|snapshot> [-data DIR]")
}
