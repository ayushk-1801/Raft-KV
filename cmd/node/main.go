package main

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ayushk-1801/raft-kv/internal/raft"
	"github.com/ayushk-1801/raft-kv/internal/store"
	"github.com/ayushk-1801/raft-kv/internal/transport"
)

type DiskStorage struct {
	mu     sync.Mutex
	nodeID int
	wal    *raft.WAL
}

func NewDiskStorage(nodeID int) *DiskStorage {
	wal, err := raft.NewWAL(fmt.Sprintf("node_%d_raft_wal.bin", nodeID))
	if err != nil {
		panic(err)
	}
	return &DiskStorage{nodeID: nodeID, wal: wal}
}

func (d *DiskStorage) getFilename(k string) string {
	return fmt.Sprintf("node_%d_%s.bin", d.nodeID, k)
}

func (d *DiskStorage) AppendLog(entry []byte) error {
	return d.wal.AppendLog(entry)
}

func (d *DiskStorage) Sync() error {
	return d.wal.Sync()
}

func (d *DiskStorage) ReadLogRange(start, end int) ([][]byte, error) {
	return d.wal.ReadLogRange(start, end)
}

func (d *DiskStorage) SaveSnapshot(meta []byte, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := os.WriteFile(d.getFilename("raft_snapshot"), data, 0644); err != nil {
		return err
	}
	return os.WriteFile(d.getFilename("raft_state"), meta, 0644)
}

func (d *DiskStorage) SaveMeta(meta []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return os.WriteFile(d.getFilename("raft_state"), meta, 0644)
}

func (d *DiskStorage) ClearLog() error {
	return d.wal.ClearLog()
}

func (d *DiskStorage) ReadState() (meta []byte, snap []byte, logs [][]byte, err error) {
	d.mu.Lock()
	meta, _ = os.ReadFile(d.getFilename("raft_state"))
	snap, _ = os.ReadFile(d.getFilename("raft_snapshot"))
	d.mu.Unlock()
	logs, err = d.wal.Load()
	return
}

func (d *DiskStorage) HasData() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	info, err := os.Stat(d.getFilename("raft_state"))
	if os.IsNotExist(err) {
		return false
	}
	return info.Size() > 0
}

type HTTPServer struct {
	node          *raft.ConsensusModule
	kv            *store.KVStore
	port          string
	peerAddresses map[int]string
	id            int
}

func (s *HTTPServer) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.node.GetStats()
	json.NewEncoder(w).Encode(stats)
}

func (s *HTTPServer) handleKV(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	key := parts[2]

	_, _, isLeader, leaderId := s.node.GetState()
	if !isLeader {
		leaderAddr, ok := s.peerAddresses[leaderId]
		if !ok {
			http.Error(w, "Leader unknown", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Location", fmt.Sprintf("http://%s/kv/%s", leaderAddr, key))
		w.WriteHeader(http.StatusTemporaryRedirect)
		return
	}

	if r.Method == http.MethodGet {
		readIndex, err := s.node.ReadBarrier()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.kv.WaitForApplyContext(ctx, readIndex); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		val, ok := s.kv.Get(key)
		if !ok {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}
		w.Write([]byte(val))
		return
	}

	var op store.Op
	op.Key = key

	if r.Method == http.MethodPost {
		var body struct{ Value string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		op.Operation = "PUT"
		op.Value = body.Value
	} else if r.Method == http.MethodDelete {
		op.Operation = "DELETE"
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	index, _, isLeader := s.node.Submit(op)
	if !isLeader {
		http.Error(w, "Leadership lost", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.kv.WaitForApplyContext(ctx, index); err != nil {
		_, _, isLeader, _ := s.node.GetState()
		if !isLeader {
			http.Error(w, "Leadership lost before apply", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Applied at index %d", index)))
}

func main() {
	gob.Register(store.Op{})
	id := flag.Int("id", -1, "Node ID")
	port := flag.String("port", "", "Port for HTTP")
	rpcPort := flag.String("rpc-port", "", "Port for Raft RPC")
	peersFlag := flag.String("peers", "", "Comma-separated peerID:host:port")
	transportType := flag.String("transport", "rpc", "rpc or grpc")
	flag.Parse()

	if *id == -1 || *port == "" {
		os.Exit(1)
	}

	if *rpcPort == "" {
		*rpcPort = *port
	}

	peerAddresses := make(map[int]string)
	var peerIds []int
	if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			parts := strings.SplitN(p, ":", 2)
			peerId, _ := strconv.Atoi(parts[0])
			peerAddresses[peerId] = parts[1]
			peerIds = append(peerIds, peerId)
		}
	}

	var grpcTransporter *transport.GrpcTransport
	var raftTransporter raft.Transporter

	if *transportType == "grpc" {
		grpcTransporter = transport.NewGrpcTransport(peerAddresses)
		raftTransporter = grpcTransporter
	} else {
		raftTransporter = transport.NewNetworkTransport(peerAddresses)
	}

	storage := NewDiskStorage(*id)
	applyCh := make(chan raft.ApplyMsg, 100)
	cm := raft.NewConsensusModule(*id, peerIds, raftTransporter, storage, applyCh)
	kv := store.NewKVStore(applyCh, cm)

	if *transportType == "grpc" {
		grpcTransporter.SetCM(cm)
		lis, _ := net.Listen("tcp", ":"+*rpcPort)
		go grpcTransporter.Serve(lis)
	} else {
		rpcProxy := transport.NewRPCProxy(cm)
		rpcServer := rpc.NewServer()
		rpcServer.Register(rpcProxy)
		rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	}

	httpServer := &HTTPServer{node: cm, kv: kv, port: *port, id: *id, peerAddresses: peerAddresses}
	http.HandleFunc("/kv/", httpServer.handleKV)
	http.HandleFunc("/stats", httpServer.handleStats)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}
