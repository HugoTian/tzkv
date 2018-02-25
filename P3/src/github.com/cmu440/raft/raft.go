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
import "../labrpc"
import (
	"time"
	"math/rand"
)
const (
	FOLLOWER int = iota
	CANDIDATE
	LEADER
)
const HEART_BEAT_TIME = 20
const LEADER_ELECTION_TIME_OUT_MIN = 100
const LEADER_ELECTION_TIME_OUT_DURATION = 400
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A go object that implement Log
type LogEntry struct {
	index int
	term int
	value string
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor int // candidate ID that voted for
	logs []LogEntry

	committedIndex int
	lastApplied int

	// volatile on leader
	nextIndex []int
	matchIndex []int

	// apply channel
	applych chan ApplyMsg

	// internal variable
	state int
	voteCount int

	// internal channels to receive message
	heartBeatCh chan bool
	voteReceivedCh chan bool
	winElecCh chan bool


}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A) .
	term = rf.currentTerm
	isleader = rf.state == LEADER

	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	term int // candidate term
	candidateId int
	lastLogIndex int
	lastLogTerm int
}

// example AppendEntries RPC args
//
type AppendEntriesArgs struct {
	term int
	leaderId int
	prevLogIndex int
	prevLogTerm int
	entries []LogEntry
	leaderCommit int //leader commit Index

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3A).
	term int
	vote bool
}


//
// example AppendEntries RPC reply
//
type AppendEntriesReply struct {
	term int
	success bool // true if follower entry matching
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// up-to-date
	if rf.currentTerm > args.term {
		reply.term = rf.currentTerm
		reply.vote = false
	} else if rf.logs[len(rf.logs)-1].index  > args.lastLogIndex {
		reply.term = rf.currentTerm
		reply.vote = false
	} else {
		reply.term = rf.currentTerm
		reply.vote = true
	}
}

//
// example AppendEntries RPC handler
//
func (rf * Raft) AppendEntries(args * AppendEntriesArgs, reply * AppendEntriesReply) {

	// handle heartBeat
	rf.heartBeatCh <- true

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
	rf.winElecCh <- reply.vote
	return ok
}

func (rf * Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
//  broadcast request vote to peers
//
func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	arg := RequestVoteArgs{}
	arg.term = rf.currentTerm
	arg.candidateId = rf.me
	arg.lastLogIndex = rf.logs[len(rf.logs)-1].index
	arg.lastLogIndex = rf.logs[len(rf.logs)-1].term

	for index := range rf.peers {
		go func (server int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &arg, &reply )
		} (index)
	}
}


//
// broadcast AppendEnrties to peers
//
func (rf * Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	arg := AppendEntriesArgs{}

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(i, &arg, &reply)
	}
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

	// Your code here (3B).


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
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.applych = applyCh
	rf.state = FOLLOWER
	// Your initialization code here (3A, 3B).
	go rf.run()

	return rf
}

func (rf*Raft) run() {
	rf.mu.Lock()
	currentState := rf.state
	switch currentState {
	case LEADER:
			go rf.broadcastAppendEntries()

	case CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm += 1
			
			rf.mu.Unlock()
			go rf.broadcastRequestVote()
			select {
				case <- rf.heartBeatCh:
					rf.mu.Lock()
					rf.state = FOLLOWER
					rf.mu.Unlock()
				case  <- rf.winElecCh:
					rf.mu.Lock()
					rf.state = LEADER
					rf.voteCount = 0
					rf.votedFor = -1
					rf.mu.Unlock()
				//no win condition
				case time.After(time.Duration(LEADER_ELECTION_TIME_OUT_MIN + rand.Int() % LEADER_ELECTION_TIME_OUT_DURATION)):
					go rf.broadcastRequestVote()
			}
	case FOLLOWER:
			select {
				case time.After(time.Duration(LEADER_ELECTION_TIME_OUT_MIN + rand.Int() % LEADER_ELECTION_TIME_OUT_DURATION)):
					rf.state = CANDIDATE
			}
	}
}