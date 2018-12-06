package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Me int64
	Method string
	Key string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database map[string]string
	appliedOp map[int64]chan int64
	appliedReq map[int64]map[int64]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.WrongLeader = true

	if kv.rf.Status() == raft.LEADER {
		kv.mu.Lock()
		if _, ok := kv.appliedOp[args.ClientId]; !ok {
			new_ch := make(chan int64, 1)
			kv.appliedOp[args.ClientId] = new_ch
		}
		kv.mu.Unlock()

		command := Op{args.ClientId, args.Me, "Get", args.Key, ""}
		kv.rf.Start(command)

		select {
		case appliedMe := <- kv.appliedOp[args.ClientId]:
			kv.mu.Lock()
			reply.Value = kv.database[args.Key]
			reply.WrongLeader = appliedMe != args.Me
			kv.mu.Unlock()
		case <-time.After(clientWaitTimeout):
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.WrongLeader = true

	if kv.rf.Status() == raft.LEADER {
		kv.mu.Lock()
		if _, ok := kv.appliedOp[args.ClientId]; !ok {
			new_ch := make(chan int64, 1)
			kv.appliedOp[args.ClientId] = new_ch
		}
		kv.mu.Unlock()

		command := Op{args.ClientId, args.Me, args.Op, args.Key, args.Value}
		kv.rf.Start(command)
		select {
		case appliedMe := <- kv.appliedOp[args.ClientId]:
			reply.WrongLeader = appliedMe != args.Me
		case <-time.After(clientWaitTimeout):
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.appliedOp = make(map[int64]chan int64)
	kv.appliedReq = make(map[int64]map[int64]int)

	go func(kv *KVServer) {
		for {
			applyMsg := <- kv.applyCh
			kv.mu.Lock()

			op := applyMsg.Command.(Op)
			if op.Method == "Put" {
				applied := make(map[int64]int)
				applied[op.Me] = 1
				kv.appliedReq[op.ClientId] = applied
				kv.database[op.Key] = op.Value
			} else if op.Method == "Append" {
				if applied, ok := kv.appliedReq[op.ClientId]; !ok {
					applied := make(map[int64]int)
					applied[op.Me] = 1
					kv.appliedReq[op.ClientId] = applied
					kv.database[op.Key] += op.Value
				} else if _, ok := applied[op.Me]; !ok {
					applied[op.Me] = 1
					kv.database[op.Key] += op.Value
				}
			}

			if _, ok := kv.appliedOp[op.ClientId]; ok {
				kv.appliedOp[op.ClientId] = make(chan int64, 1)
				kv.appliedOp[op.ClientId] <- op.Me
			}

			kv.mu.Unlock()
		}
	} (kv)

	return kv
}

const (
	clientWaitTimeout = 200 * time.Millisecond
)
