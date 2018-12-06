package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
// lab1
// import "fmt"
import "time"
import "math/rand"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// constants for state
const (
	Follower = 0
	Candidate = 1
	Leader = 2
)

// constants for timeout
const (
	ElectionTimeout = 1 * time.Microsecond
	HeartbeatTimeout = 100 * time.Millisecond
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (Lab1, Lab2, Challenge1).
	// Lab1
	status int
	currentTerm int

	votedFor bool
	numVotes int

	AppendEntriesRPC chan int
	RequestVoteRPC chan int
	AppendEntriesSuccess chan int
	RequestVoteSuccess chan int
	
	log []int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (Lab1).
	term = rf.currentTerm
	isleader = false
	if rf.status == Leader {
		isleader = true
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (Challenge1).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (Challenge1).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// Lab1
func (rf *Raft) makeEmptyChannel() {
	for len(rf.RequestVoteSuccess) > 0 {
		<- rf.RequestVoteSuccess
	}
	for len(rf.RequestVoteRPC) > 0 {
		<- rf.RequestVoteRPC
	}
	for len(rf.AppendEntriesSuccess) > 0 {
		<- rf.AppendEntriesSuccess
	}
	for len(rf.AppendEntriesRPC) > 0 {
		<- rf.AppendEntriesRPC
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (Lab1, Lab2).
	Term int
	CadidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (Lab1).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (Lab1, Lab2).
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = false
		rf.makeEmptyChannel()
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	rf_LastLogIndex := len(rf.log)-1
	rf_LastLogTerm := 0
	if rf_LastLogIndex > 0 {
		rf_LastLogTerm = rf.log[rf_LastLogIndex]
	}

	if rf.votedFor == false {
		if args.LastLogTerm > rf_LastLogTerm {
			reply.VoteGranted = true
			rf.votedFor = true
		} else if args.LastLogTerm == rf_LastLogTerm {
			if args.LastLogIndex >= rf_LastLogIndex {
				reply.VoteGranted = true
				rf.votedFor = true
			}
		}
	}

	rf.RequestVoteRPC <- 1
	rf.mu.Unlock()
}

// lab1 : AppendEntries
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	for len(rf.AppendEntriesRPC) > 0 {
		<- rf.AppendEntriesRPC
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.currentTerm
	rf.AppendEntriesRPC <- 1
	reply.Success = true
	rf.status = Follower
	rf.votedFor = false
	for len(rf.RequestVoteSuccess) > 0 {
		<- rf.RequestVoteSuccess
	}
	for len(rf.RequestVoteRPC) > 0 {
		<- rf.RequestVoteRPC
	}
	for len(rf.AppendEntriesSuccess) > 0 {
		<- rf.AppendEntriesSuccess
	}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	if ok == false || args.Term != rf.currentTerm || rf.status != Candidate || rf.votedFor == false {
		rf.mu.Unlock()
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = false
		for len(rf.RequestVoteSuccess) > 0 {
			<- rf.RequestVoteSuccess
		}
		for len(rf.RequestVoteRPC) > 0 {
			<- rf.RequestVoteRPC
		}
		for len(rf.AppendEntriesSuccess) > 0 {
			<- rf.AppendEntriesSuccess
		}
		for len(rf.AppendEntriesRPC) > 0 {
			<- rf.AppendEntriesRPC
		}
		rf.mu.Unlock()
		return ok
	}

	if reply.VoteGranted == true {
		rf.numVotes = rf.numVotes + 1
	}
	if rf.numVotes > len(rf.peers)/2 {
		rf.RequestVoteSuccess <- 1
	}
	rf.mu.Unlock()
	return ok
}

// lab1 : sendAppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok == false || args.Term != rf.currentTerm || rf.status != Leader {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = false
		for len(rf.RequestVoteSuccess) > 0 {
			<- rf.RequestVoteSuccess
		}
		for len(rf.RequestVoteRPC) > 0 {
			<- rf.RequestVoteRPC
		}
		for len(rf.AppendEntriesSuccess) > 0 {
			<- rf.AppendEntriesSuccess
		}
		for len(rf.AppendEntriesRPC) > 0 {
			<- rf.AppendEntriesRPC
		}
		return ok
	}
	if reply.Success == true {
		rf.AppendEntriesSuccess <- 1
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (Lab2).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (Lab1, Lab2, Challenge1).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.status = Follower
	rf.currentTerm = 0
	rf.votedFor = false
	rf.AppendEntriesRPC = make(chan int, 1)
	rf.RequestVoteRPC = make(chan int, 1)
	rf.AppendEntriesSuccess = make(chan int, 1)
	rf.RequestVoteSuccess = make(chan int, 1)

	go func() {
		for {
			if rf.status == Follower {
				random_factor := time.Duration(rand.Intn(200000)+300000)
				select {
				case <-rf.AppendEntriesRPC:
				case <-rf.RequestVoteRPC:
				case <-time.After(ElectionTimeout * random_factor):
					rf.mu.Lock()
					rf.currentTerm = rf.currentTerm + 1
					rf.status = Candidate
					rf.numVotes = 1
					rf.votedFor = true
					rf.mu.Unlock()
				}
			} else if rf.status == Candidate {
				rf.mu.Lock()
				rf.numVotes = 1
				rf.votedFor = true
				arg_term := rf.currentTerm
				arg_lastindex := len(rf.log)
				arg_lastterm := 0
				if len(rf.log) > 0 {
					arg_lastterm = rf.log[len(rf.log)-1]
				}
				rf.mu.Unlock()

				for peer := range rf.peers {
					if peer != me {
						var requestArgs RequestVoteArgs
						var requestReply RequestVoteReply
						requestArgs.Term = arg_term
						requestArgs.LastLogIndex = arg_lastindex
						requestArgs.LastLogTerm = arg_lastterm

						go rf.sendRequestVote(peer, &requestArgs, &requestReply)
					}
				}

				random_factor := time.Duration(rand.Intn(150000)+150000)
				select {
				case <-rf.AppendEntriesRPC:
				case <-rf.RequestVoteSuccess:
					rf.status = Leader
				case <-time.After(ElectionTimeout * random_factor):
					rf.currentTerm = rf.currentTerm + 1
				}
			} else if rf.status == Leader {
				select {
				case <-time.After(HeartbeatTimeout):
					rf.mu.Lock()
					arg_term := rf.currentTerm

					for peer := range rf.peers {
						if peer != me {
							var requestArgs AppendEntriesArgs
							var requestReply AppendEntriesReply

							requestArgs.Term = arg_term
							go rf.sendAppendEntries(peer, &requestArgs, &requestReply)
						}
					}
					rf.mu.Unlock()
				}
			}
		}
	} ()

	return rf
}
