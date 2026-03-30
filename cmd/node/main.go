package main

import (
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

	"github.com/ayushk-1801/raft-kv/internal/raft"
	"github.com/ayushk-1801/raft-kv/internal/store"
	"github.com/ayushk-1801/raft-kv/internal/transport"
)

type DiskStorage struct {
	mu     sync.Mutex
	nodeID int
}

func NewDiskStorage(nodeID int) *DiskStorage {
	return &DiskStorage{nodeID: nodeID}
}

func (d *DiskStorage) getFilename(k string) string {
	return fmt.Sprintf("node_%d_%s.bin", d.nodeID, k)
}

func (d *DiskStorage) Set(k string, v []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return os.WriteFile(d.getFilename(k), v, 0644)
}

func (d *DiskStorage) Get(k string) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return os.ReadFile(d.getFilename(k))
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

	s.kv.WaitForApply(index)
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
		fmt.Println("Usage: go run main.go -id <id> -port <port> [-rpc-port <port>] [-transport rpc|grpc] [-peers <id:host:port>,...]")
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

	var transporter raft.Transporter
	var grpcTransporter *transport.GrpcTransport

	if *transportType == "grpc" {
		grpcTransporter = transport.NewGrpcTransport(peerAddresses)
		transporter = grpcTransporter
	} else {
		transporter = transport.NewNetworkTransport(peerAddresses)
	}

	storage := NewDiskStorage(*id)
	applyCh := make(chan raft.ApplyMsg, 100)

	cm := raft.NewConsensusModule(*id, peerIds, transporter, storage, applyCh)
	kv := store.NewKVStore(applyCh, cm)

	if *transportType == "grpc" {
		grpcTransporter.SetCM(cm)
		lis, err := net.Listen("tcp", ":"+*rpcPort)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		go grpcTransporter.Serve(lis)
	} else {
		rpcProxy := transport.NewRPCProxy(cm)
		rpcServer := rpc.NewServer()
		rpcServer.Register(rpcProxy)
		rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	}

	httpServer := &HTTPServer{
		node:          cm,
		kv:            kv,
		port:          *port,
		id:            *id,
		peerAddresses: peerAddresses,
	}
	http.HandleFunc("/kv/", httpServer.handleKV)
	http.HandleFunc("/stats", httpServer.handleStats)

	fmt.Printf("Node %d booting. HTTP on %s, RPC on %s (%s). Peers: %v\n", *id, *port, *rpcPort, *transportType, peerIds)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}
