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

import (
	"bytes"
	"fmt"
	"labgob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (Lab1, Lab2, Challenge1).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	status Status // server state (follower, candidate, or leader)

	voteCount int // count of votes for the leader election

	// Persistent state on all servers
	currentTerm int // latest term server has seen
	votedFor int // candidadeId that received vote in current term
	log[] Log // log entries

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leader
	nextIndex[] int // for each server, index of the next log entry to send to that server
	matchIndex[] int // for each server, index of highest log entry to be replicated on server

	// Channels requiered for server notification
	applyCh chan ApplyMsg
	electionWonCh chan  int
	heartbeatCh chan int
}

func (rf *Raft) resetAsFollower(term int){
	rf.status=FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = term
}

type Status int

const (
	FOLLOWER Status = 0
	CANDIDATE Status = 1
	LEADER Status = 2
)

type Log struct {
	Term int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (Lab1).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.status == LEADER
	rf.mu.Unlock()
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
	w:= new (bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log[] Log
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil{
		fmt.Printf("[Server %d]: error reading saved data\n", rf.me)
	}else{
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

func compareInt(a, b int) int {
	if a>b{
		return 1
	}
	if a<b{
		return -1
	}
	return 0
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (Lab1, Lab2).
	Term int // candidate's term
	CandidateId int // candidate requesting vote
	LastLogIndex int // index of candidate's log entry
	LastLogTerm int // term of candidate's log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (Lab1).
	Term int // cuurentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// Check if the candidate logs are at least as up-to-date as server logs
func (rf *Raft) checkLogs(args *RequestVoteArgs) bool {
	switch compareInt(rf.log[len(rf.log)-1].Term, args.LastLogTerm){
	case -1:
		return true
	case 0:
		if len(rf.log)-1 <= args.LastLogIndex{
			return true
		}
		fallthrough
	default:
		return false
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (Lab1, Lab2).
	rf.mu.Lock()
	switch compareInt(rf.currentTerm, args.Term) {
	case 1: // candidate outdated
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	case -1: // server outdated
		rf.resetAsFollower(args.Term)
		rf.persist()
		fallthrough
	default:
		reply.Term = args.Term
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.checkLogs(args){
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}else{
			reply.VoteGranted = false
		}
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
	if !(rf.status == CANDIDATE && rf.currentTerm == args.Term) {
		rf.mu.Unlock()
		return ok
	}

	if ok {
		switch compareInt(rf.currentTerm, reply.Term) {
		case 1: // wrong election
		case -1: // candidate outdated
			rf.resetAsFollower(reply.Term)
			rf.persist()
		default:
			if reply.VoteGranted{
				rf.voteCount++
				if rf.voteCount > len(rf.peers)/2{
					rf.status = LEADER
					rf.electionWonCh <- 1 // notify the candidate that he won
					// Create volatile leader state
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					
					for peer := range rf.peers {
						rf.nextIndex[peer] = len(rf.log)
						rf.matchIndex[peer] = len(rf.log)-1
					}
				}
			}
		}
	}
	rf.mu.Unlock()
	return ok
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries[] Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	UpdateIndex int
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	switch compareInt(rf.currentTerm, args.Term) {
	case 1: // outdated leader
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	case -1: // outdated server
		rf.resetAsFollower(args.Term)
		reply.Term = rf.currentTerm
		rf.persist()
		fallthrough
	default:
		rf.heartbeatCh <- 1 // trigger heartbeat reception from leader
		// TODO lab2: update logs
		if len(rf.log)-1 < args.PrevLogIndex {
			reply.UpdateIndex = len(rf.log)
			rf.mu.Unlock()
			return
		}

		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			updateIndex := args.PrevLogIndex
			for updateIndex > 0 && rf.log[updateIndex].Term == rf.log[args.PrevLogIndex].Term {
				updateIndex--
			}

			reply.UpdateIndex = updateIndex+1
			rf.mu.Unlock()
			return
		}

		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		if rf.commitIndex < args.LeaderCommit {
			switch compareInt(len(rf.log)-1, args.LeaderCommit) {
			case 1:
				rf.commitIndex = args.LeaderCommit
			default:
				rf.commitIndex = len(rf.log)-1
			}
		}

		reply.UpdateIndex = len(rf.log)
		rf.updateApplyIndex()
		rf.persist()
		rf.mu.Unlock()
		return
	}
}

// Send an AppendEntries to a server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	if !(ok && rf.status == LEADER && rf.currentTerm == args.Term) {
		rf.mu.Unlock()
		return ok
	}

	switch compareInt(rf.currentTerm, reply.Term) {
		case -1: // outdated leader
			rf.resetAsFollower(reply.Term)
			rf.persist()
		default:
			rf.matchIndex[server] = reply.UpdateIndex-1
			rf.nextIndex[server] = reply.UpdateIndex
	}

	rf.mu.Unlock()
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
	rf.mu.Lock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.status == LEADER

	// Your code here (Lab2).
	if isLeader {
		index = len(rf.log)
		rf.log = append(rf.log, Log{term, command})
		rf.persist()
	}

	rf.mu.Unlock()
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

func (rf *Raft) updateCommitIndex() {
	commitIndex := rf.commitIndex+1

	for commitIndex <= len(rf.log)-1 {
		matchCnt := 0
		for server := range rf.peers {
			if server != rf.me && rf.log[commitIndex].Term == rf.currentTerm {
				if rf.matchIndex[server] >= commitIndex {
					matchCnt++
				}
			}
		}

		if matchCnt >= len(rf.peers)/2 {
			rf.commitIndex = commitIndex
		}
		commitIndex++
	}
}

func (rf *Raft) updateApplyIndex() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}
	}
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
	rf.status = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	EmptyLog := &Log{Term: 0} // empty log used to fill the hole before first real log
	rf.log = append(rf.log, *EmptyLog)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.heartbeatCh = make(chan int, len(rf.peers))
	rf.electionWonCh = make(chan int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// infinite loop running on top of 
	go func(rf *Raft){
		for {
			// fmt.Printf("[Server %d]: state %d, term %d, voteFor %d\n", rf.me, rf.state, rf.currentTerm, rf.votedFor)
			switch rf.status {
			case FOLLOWER:
				timer := time.NewTimer(ElectionTimeout())
				select{
				case <-rf.heartbeatCh:
				case <- timer.C:
					rf.status = CANDIDATE
				}
			case CANDIDATE:
				// Init candidate state
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()

				// Send requestvotes to other servers
				go func (rf *Raft){
					// Build args to send
					args := &RequestVoteArgs{}
					args.Term = rf.currentTerm
					args.CandidateId = rf.me
					args.LastLogIndex = len(rf.log)-1
					args.LastLogTerm = rf.log[len(rf.log)-1].Term

					for server := range rf.peers {
						if server != rf.me {
							go rf.sendRequestVote(server, args, &RequestVoteReply{})
						}
					}
				}(rf)

				timer := time.NewTimer(ElectionTimeout())
				select {
				case <- rf.heartbeatCh:
					rf.mu.Lock()
					rf.resetAsFollower(rf.currentTerm)
					rf.persist()
					rf.mu.Unlock()
				case <- rf.electionWonCh:
					// fmt.Printf("[Server %d]: New leader\n",rf.me)
				case <- timer.C:
				}
			case LEADER:
				// Send heartbeats to other servers
				go func (rf *Raft){
					for server := range rf.peers{
						if server != rf.me{
							rf.mu.Lock()
							args := &AppendEntriesArgs{}
							args.Term = rf.currentTerm
							args.LeaderId = rf.me
							args.PrevLogIndex = rf.nextIndex[server]-1
							args.PrevLogTerm = rf.log[rf.nextIndex[server]-1].Term
							args.LeaderCommit = rf.commitIndex
							args.Entries = rf.log[rf.nextIndex[server]:]
							rf.mu.Unlock()
							go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
						}
					}

					rf.mu.Lock()
					rf.updateCommitIndex()
					rf.updateApplyIndex()
					rf.mu.Unlock()
				}(rf)
				time.Sleep(heartbeatTimeout)
			}
		}
	}(rf)

	return rf
}

const (
	heartbeatTimeout = 300 * time.Millisecond // less than 10 heartbeats per second
	electionTimeoutLowerBound int = 400
	electionTimeoutUpperBound int = 1000
)

func ElectionTimeout() time.Duration{
	return time.Duration(electionTimeoutLowerBound + rand.Intn(electionTimeoutUpperBound-electionTimeoutLowerBound)) * time.Millisecond
}