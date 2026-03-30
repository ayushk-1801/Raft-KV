package store

import (
	"bytes"
	"encoding/gob"
	"sync"
	"github.com/ayushk-1801/raft-kv/internal/raft"
)

type Op struct {
	Operation string 
	Key       string
	Value     string
}

type KVStore struct {
	mu sync.RWMutex
	db map[string]string

	applyCh <-chan raft.ApplyMsg
	node    *raft.ConsensusModule
	
	lastApplied int
	applyCond   *sync.Cond
}

func NewKVStore(applyCh <-chan raft.ApplyMsg, node *raft.ConsensusModule) *KVStore {
	kv := &KVStore{
		db:      make(map[string]string),
		applyCh: applyCh,
		node:    node,
	}
	kv.applyCond = sync.NewCond(&kv.mu)
	go kv.readApplyCh()
	return kv
}

func (kv *KVStore) Snapshot() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(kv.db); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (kv *KVStore) RestoreFromSnapshot(data []byte) {
	var db map[string]string
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&db); err != nil {
		panic(err)
	}
	kv.db = db
}

func (kv *KVStore) readApplyCh() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			if msg.Command == "SNAPSHOT" {
				kv.RestoreFromSnapshot(msg.Data)
				kv.mu.Lock()
				kv.lastApplied = msg.CommandIndex
				kv.applyCond.Broadcast()
				kv.mu.Unlock()
			}
			continue
		}

		op, ok := msg.Command.(Op)
		if ok {
			kv.mu.Lock()
			switch op.Operation {
			case "PUT":
				kv.db[op.Key] = op.Value
			case "DELETE":
				delete(kv.db, op.Key)
			}
			
			kv.lastApplied = msg.CommandIndex
			kv.applyCond.Broadcast()

			if msg.CommandIndex % 100 == 0 {
				data := kv.Snapshot()
				go kv.node.Snapshot(msg.CommandIndex, data)
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVStore) WaitForApply(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for kv.lastApplied < index {
		kv.applyCond.Wait()
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.db[key]
	return val, ok
}
