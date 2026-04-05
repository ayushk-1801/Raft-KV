package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ayushk-1801/raft-kv/internal/raft"
	"github.com/ayushk-1801/raft-kv/internal/store"
	"github.com/ayushk-1801/raft-kv/internal/transport"
)

type DiskStorage struct {
	mu           sync.Mutex
	nodeID       int
	wal          *raft.WAL
	walPath      string
	snapshotPath string
}

type diskStateManifest struct {
	Meta         []byte
	SnapshotFile string
	WALFile      string
}

func NewDiskStorage(nodeID int) *DiskStorage {
	ds := &DiskStorage{
		nodeID:       nodeID,
		walPath:      fmt.Sprintf("node_%d_raft_wal.bin", nodeID),
		snapshotPath: fmt.Sprintf("node_%d_raft_snapshot.bin", nodeID),
	}

	if manifest, ok := ds.readManifest(); ok {
		if manifest.WALFile != "" {
			ds.walPath = manifest.WALFile
		}
		if manifest.SnapshotFile != "" {
			ds.snapshotPath = manifest.SnapshotFile
		}
	}

	wal, err := raft.NewWAL(ds.walPath)
	if err != nil {
		panic(err)
	}
	ds.wal = wal
	return ds
}

func (d *DiskStorage) getFilename(k string) string {
	return fmt.Sprintf("node_%d_%s.bin", d.nodeID, k)
}

func (d *DiskStorage) statePath() string {
	return d.getFilename("raft_state")
}

func (d *DiskStorage) versionedPath(kind string) string {
	return fmt.Sprintf("node_%d_%s_%d.bin", d.nodeID, kind, time.Now().UnixNano())
}

func (d *DiskStorage) manifestBytes(meta []byte, snapshotPath string, walPath string) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(diskStateManifest{
		Meta:         meta,
		SnapshotFile: snapshotPath,
		WALFile:      walPath,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func atomicWriteFile(path string, data []byte) error {
	tmpPath := fmt.Sprintf("%s.tmp.%d", path, time.Now().UnixNano())
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func writeWALFile(path string, entries [][]byte) error {
	tmpPath := fmt.Sprintf("%s.tmp.%d", path, time.Now().UnixNano())
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		var sz [4]byte
		binary.BigEndian.PutUint32(sz[:], uint32(len(entry)))
		if _, err := f.Write(sz[:]); err != nil {
			f.Close()
			return err
		}
		if _, err := f.Write(entry); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func loadWALFile(path string) ([][]byte, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}

	var entries [][]byte
	for {
		var sz [4]byte
		if _, err := io.ReadFull(f, sz[:]); err != nil {
			if err == io.EOF {
				break
			}
			if err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		length := binary.BigEndian.Uint32(sz[:])
		data := make([]byte, length)
		if _, err := io.ReadFull(f, data); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		entries = append(entries, data)
	}

	return entries, nil
}

func (d *DiskStorage) readManifest() (diskStateManifest, bool) {
	raw, err := os.ReadFile(d.statePath())
	if err != nil {
		return diskStateManifest{}, false
	}

	var manifest diskStateManifest
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&manifest); err != nil || manifest.Meta == nil {
		return diskStateManifest{
			Meta:         raw,
			SnapshotFile: d.getFilename("raft_snapshot"),
			WALFile:      d.getFilename("raft_wal"),
		}, true
	}
	return manifest, true
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
	logs, err := d.wal.Load()
	if err != nil {
		d.mu.Unlock()
		return err
	}
	defer d.mu.Unlock()
	return d.replaceStateLocked(meta, data, logs)
}

func (d *DiskStorage) SaveMeta(meta []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	manifestData, err := d.manifestBytes(meta, d.snapshotPath, d.walPath)
	if err != nil {
		return err
	}
	return atomicWriteFile(d.statePath(), manifestData)
}

func (d *DiskStorage) replaceStateLocked(meta []byte, snapshot []byte, logs [][]byte) error {
	newSnapshotPath := d.versionedPath("raft_snapshot")
	newWALPath := d.versionedPath("raft_wal")

	if err := atomicWriteFile(newSnapshotPath, snapshot); err != nil {
		return err
	}
	if err := writeWALFile(newWALPath, logs); err != nil {
		return err
	}
	newWAL, err := raft.NewWAL(newWALPath)
	if err != nil {
		return err
	}

	manifestData, err := d.manifestBytes(meta, newSnapshotPath, newWALPath)
	if err != nil {
		newWAL.Close()
		return err
	}
	if err := atomicWriteFile(d.statePath(), manifestData); err != nil {
		newWAL.Close()
		return err
	}

	oldWALPath := d.walPath
	oldSnapshotPath := d.snapshotPath
	oldWAL := d.wal
	d.wal = newWAL
	d.walPath = newWALPath
	d.snapshotPath = newSnapshotPath

	if oldWAL != nil {
		oldWAL.Close()
	}
	if oldWALPath != "" && oldWALPath != newWALPath {
		_ = os.Remove(oldWALPath)
	}
	if oldSnapshotPath != "" && oldSnapshotPath != newSnapshotPath {
		_ = os.Remove(oldSnapshotPath)
	}
	return nil
}

func (d *DiskStorage) ReplaceState(meta []byte, snapshot []byte, logs [][]byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.replaceStateLocked(meta, snapshot, logs)
}

func (d *DiskStorage) ClearLog() error {
	return d.wal.ClearLog()
}

func (d *DiskStorage) ReadState() (meta []byte, snap []byte, logs [][]byte, err error) {
	d.mu.Lock()
	manifest, ok := d.readManifest()
	d.mu.Unlock()
	if !ok {
		logs, err = d.wal.Load()
		return nil, nil, logs, err
	}

	meta = append([]byte(nil), manifest.Meta...)
	if manifest.SnapshotFile != "" {
		snap, err = os.ReadFile(manifest.SnapshotFile)
		if err != nil && !os.IsNotExist(err) {
			return nil, nil, nil, err
		}
	}
	logs, err = loadWALFile(manifest.WALFile)
	return meta, snap, logs, err
}

func (d *DiskStorage) HasData() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, path := range []string{d.statePath(), d.walPath, d.snapshotPath} {
		info, err := os.Stat(path)
		if err == nil && info.Size() > 0 {
			return true
		}
	}
	return false
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
