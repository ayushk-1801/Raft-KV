package bench

import (
	"encoding/gob"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"testing"
	"time"

	"github.com/ayushk-1801/raft-kv/internal/raft"
	"github.com/ayushk-1801/raft-kv/internal/store"
	"github.com/ayushk-1801/raft-kv/internal/transport"
)

func init() {
	gob.Register(store.Op{})
}

type benchCluster struct {
	cms     []*raft.ConsensusModule
	servers []*http.Server
	grpc    []*transport.GrpcTransport
}

func (c *benchCluster) shutdown() {
	for _, cm := range c.cms {
		cm.Kill()
	}
	for _, s := range c.servers {
		if s != nil {
			s.Close()
		}
	}
}

func setupCluster(n int, transportType string) (*benchCluster, int) {
	peerAddresses := make(map[int]string)
	ports := make([]int, n)
	for i := 0; i < n; i++ {
		l, _ := net.Listen("tcp", "localhost:0")
		ports[i] = l.Addr().(*net.TCPAddr).Port
		peerAddresses[i] = fmt.Sprintf("localhost:%d", ports[i])
		l.Close()
	}

	cluster := &benchCluster{
		cms:     make([]*raft.ConsensusModule, n),
		servers: make([]*http.Server, n),
		grpc:    make([]*transport.GrpcTransport, n),
	}

	for i := 0; i < n; i++ {
		others := make(map[int]string)
		var otherIds []int
		for j := 0; j < n; j++ {
			if i != j {
				others[j] = peerAddresses[j]
				otherIds = append(otherIds, j)
			}
		}

		var transporter raft.Transporter
		if transportType == "grpc" {
			gt := transport.NewGrpcTransport(others)
			transporter = gt
			cluster.grpc[i] = gt
		} else {
			transporter = transport.NewNetworkTransport(others)
		}

		storage := &mockStorage{data: make(map[string][]byte)}
		applyCh := make(chan raft.ApplyMsg, 1000)
		cm := raft.NewConsensusModule(i, otherIds, transporter, storage, applyCh)
		cluster.cms[i] = cm

		if transportType == "grpc" {
			cluster.grpc[i].SetCM(cm)
			lis, _ := net.Listen("tcp", fmt.Sprintf("localhost:%d", ports[i]))
			go cluster.grpc[i].Serve(lis)
		} else {
			rpcProxy := transport.NewRPCProxy(cm)
			server := rpc.NewServer()
			server.Register(rpcProxy)
			mux := http.NewServeMux()
			mux.Handle(rpc.DefaultRPCPath, server)

			hs := &http.Server{
				Addr:    fmt.Sprintf("localhost:%d", ports[i]),
				Handler: mux,
			}
			cluster.servers[i] = hs
			go hs.ListenAndServe()
		}
	}

	time.Sleep(2 * time.Second)

	leader := -1
	for i := 0; i < n; i++ {
		_, term, isLeader, _ := cluster.cms[i].GetState()
		if isLeader {
			fmt.Printf("Node %d is leader for term %d\n", i, term)
			leader = i
			break
		}
	}
	return cluster, leader
}

func runBenchmark(b *testing.B, transportType string, useSnapshots bool) {
	cluster, leader := setupCluster(3, transportType)
	defer cluster.shutdown()

	if leader == -1 {
		b.Fatal("No leader elected")
	}

	var mu sync.Mutex
	waiters := make(map[int]chan struct{})

	go func() {
		applyCh := cluster.cms[leader].GetApplyCh()
		for msg := range applyCh {
			if msg.CommandValid {
				mu.Lock()
				if ch, ok := waiters[msg.CommandIndex]; ok {
					close(ch)
					delete(waiters, msg.CommandIndex)
				}
				mu.Unlock()
			}

			if useSnapshots && msg.CommandIndex%100 == 0 {
				cluster.cms[leader].Snapshot(msg.CommandIndex, []byte("snap"))
			}
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			op := store.Op{Operation: "PUT", Key: "k", Value: "v"}
			index, _, isLeader := cluster.cms[leader].Submit(op)
			if !isLeader {
				return
			}

			ch := make(chan struct{})
			mu.Lock()
			waiters[index] = ch
			mu.Unlock()

			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				return
			}
		}
	})
	b.StopTimer()

	fmt.Printf("\n--- %s (Snapshots: %v) ---\n", transportType, useSnapshots)
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(b.N)/b.Elapsed().Seconds())
}

func BenchmarkRPC(b *testing.B) {
	runBenchmark(b, "rpc", false)
}

func BenchmarkGRPC(b *testing.B) {
	runBenchmark(b, "grpc", false)
}

func BenchmarkRPCWithSnapshots(b *testing.B) {
	runBenchmark(b, "rpc", true)
}

func BenchmarkGRPCWithSnapshots(b *testing.B) {
	runBenchmark(b, "grpc", true)
}

type mockStorage struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (ms *mockStorage) AppendLog(entry []byte) error                  { return nil }
func (ms *mockStorage) Sync() error                                   { return nil }
func (ms *mockStorage) ReadLogRange(start, end int) ([][]byte, error) { return nil, nil }
func (ms *mockStorage) SaveSnapshot(meta, data []byte) error          { return nil }
func (ms *mockStorage) SaveMeta(meta []byte) error                    { return nil }
func (ms *mockStorage) ClearLog() error                               { return nil }
func (ms *mockStorage) ReadState() (meta []byte, snap []byte, logs [][]byte, err error) {
	return nil, nil, nil, nil
}
func (ms *mockStorage) Set(key string, value []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.data[key] = value
	return nil
}
func (ms *mockStorage) Get(key string) ([]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.data[key], nil
}
func (ms *mockStorage) HasData() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.data) > 0
}
