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
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

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

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
	applyCh    chan ApplyMsg

	votes          int
	validServers   int
	state          int
	resetChan      chan int
	ElecTimeout    time.Duration
	HeartInterval  time.Duration
	HeartbeatTimer *time.Timer
}

func (rf *Raft) RFInit() {
	rf.ElecTimeout = time.Millisecond * time.Duration(rand.Intn(150)+150)
	rf.HeartInterval = time.Millisecond * time.Duration(85)
	//rf.ElectionTimer = time.NewTimer(rf.ElecTimeout)
	rf.HeartbeatTimer = time.NewTimer(rf.HeartInterval)
	rf.resetChan = make(chan int, 1)
	rf.state = FollowerState

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry,0,1)
	rf.log = append(rf.log, LogEntry{0,1313})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for idx,_ := range rf.matchIndex{
		rf.matchIndex[idx] = 0
	}

	//rf.persist()
}

func (rf *Raft) BecomeCandidate() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.state = CandidateState
	rf.votedFor = rf.me
	rf.votes = 1
	rf.validServers = 1
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getlastindex(),
		rf.log[rf.getlastindex()].Term,
	}
	//rf.ElectionTimer.Reset(rf.ElecTimeout)
	rf.persist()
	rf.mu.Unlock()
	rf.StartElection(args)
	//rf.ElectionRoutine()
}

func (rf *Raft) BecomeFollower(newTerm int) {
	rf.state = FollowerState
	rf.currentTerm = newTerm
	rf.persist()
	//rf.ElectionTimer.Reset(rf.ElecTimeout)
}

func (rf *Raft) BecomeLeader() {
	rf.state = LeaderState
	tmpval := rf.getlastindex()
	for idx,_ := range rf.nextIndex{
		rf.nextIndex[idx] = tmpval + 1
		rf.matchIndex[idx] = 0
	}
	rf.persist()
}

func (rf *Raft) GenericRoutine() {
	go func() {
		for true {
			rf.mu.Lock()
			curState := rf.state
			rf.mu.Unlock()
			if curState == FollowerState {
				//fmt.Println(rfme, " GenericRoutine, FollowerState")
				select {
				case <-time.After(rf.ElecTimeout):
					rf.BecomeCandidate()
				case <-rf.resetChan:
					continue
				}
			}
			if curState == CandidateState {
				//fmt.Println(rfme, " GenericRoutine, CandidateState")
				select {
				case <-time.After(rf.ElecTimeout):
					rf.mu.Lock()
					curState = rf.state
					rf.mu.Unlock()
					if curState != CandidateState {
						continue
					}
					rf.BecomeCandidate()
				case <-rf.resetChan:
					continue
				}
			}
			if curState == LeaderState {
				//fmt.Println(rfme, " GenericRoutine, LeaderState")
				rf.SyncEntries()
				//rf.mu.Lock()
				//rf.HeartbeatTimer.Reset(rf.HeartInterval)
				//rf.mu.Unlock()
				//_ = <-rf.HeartbeatTimer.C
				time.Sleep(rf.HeartInterval)
			}
		}
	}()
}

func (rf *Raft) UpdateCommitIdx() {
	N := rf.commitIndex + 1
	newN := -1
	for N <= rf.getlastindex() {
		if rf.log[N].Term != rf.currentTerm {
			N++
			continue
		}
		num := 0.0
		for _,matchVal := range rf.matchIndex{
			if matchVal >= N{
				num++
			}
		}
		if num > 0.5 * float64(len(rf.peers)){
			newN = N
			N++
			continue
		} else {
			break
		}
	}
	if newN != -1{
		rf.commitIndex = newN
	}
}

func (rf *Raft) ApplyCmd() {
	for rf.lastApplied < rf.commitIndex{
		rf.lastApplied++
		msg := ApplyMsg{
			rf.lastApplied,
			rf.log[rf.lastApplied].Command,
			false,
			make([]byte,0,1),
		}
		//fmt.Println(rf.me," apply msg: ",rf.lastApplied,",",rf.log[rf.lastApplied].Command)
		rf.applyCh <- msg
	}
}

func (rf *Raft)print_entries(){
	var server string
	if rf.state == LeaderState{
		server = "Leader"
	} else {
		server = "Follower"
	}
	fmt.Print(server,rf.me," term: ",rf.currentTerm," log: ")
	for idx,logentry := range rf.log{
		fmt.Print("[",idx,",",logentry.Term,",",logentry.Command,"]")
	}
	fmt.Println()
	//fmt.Println(rf.me," lastApplied: ",rf.matchIndex[0],)
}

func (rf *Raft)print_args_entries(prevlogindex int ,entries []LogEntry){
	if len(entries) == 0{
		return
	}
	print("server: ",rf.me," arg entries: ")
	for idx,ent := range entries{
		print("[",prevlogindex+idx,",",ent.Term,"]")
	}
	println()
}

func (rf *Raft) SyncEntries() {
	for i := 0; i < len(rf.peers); i++ {
		rf.mu.Lock()
		rfme := rf.me
		rf.mu.Unlock()
		if i == rfme {
			continue
		}
		go func(idx int) {
			for true{
				rf.mu.Lock()
				prevlogindex := rf.nextIndex[idx] - 1
				prevlogterm := rf.log[prevlogindex].Term
				var entries []LogEntry
				if prevlogindex == rf.getlastindex(){
					entries = make([]LogEntry,0)
				} else {
					entries = rf.log[prevlogindex+1:]
				}
				//rf.print_args_entries(prevlogindex,entries)
				args := AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevlogindex,
					prevlogterm,
					entries,
					rf.commitIndex,
				}
				//fmt.Println(rf.me, " SendHeartbeats, term: ", rf.currentTerm)
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(idx, args, reply)
				rf.mu.Lock()
				if !ok || rf.state != LeaderState {
					rf.mu.Unlock()
					return
				}
				if ok {
					if reply.Term > rf.currentTerm {
						rf.BecomeFollower(reply.Term)
						rf.mu.Unlock()
						return
					}
					if reply.Success{
						rf.matchIndex[idx] = prevlogindex + len(entries)
						rf.nextIndex[idx] = rf.matchIndex[idx] + 1 //len(entries)
						//fmt.Println("matchIndex: ")
						//for idx,val := range rf.matchIndex{
						//	print(idx,":",val,"  ")
						//}
						//fmt.Println()
						rf.UpdateCommitIdx()
						rf.ApplyCmd()
					} else {
						rf.nextIndex[idx] -= 1
						//fmt.Println("leader: ",rf.me," nextIndex[",idx,"]-1 --> ",rf.nextIndex[idx])
					}
					if rf.matchIndex[idx] == rf.getlastindex(){  // synchronized
						rf.mu.Unlock()
						return
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) StartElection(args RequestVoteArgs) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(idx, args, reply)
			if ok {
				rf.mu.Lock()
				rf.validServers++
				if rf.state != CandidateState {
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					rf.BecomeFollower(reply.Term)
				} else if reply.VoteGranted == true {
					//fmt.Println(rf.me, " gets vote from ", idx, "in term ", rf.currentTerm)
					rf.votes++
					if float64(rf.votes) > 0.5*float64(len(rf.peers)) {
						rf.BecomeLeader()
						fmt.Println(rf.me, " BecomeLeader 1 in term ", rf.currentTerm)
						rf.resetChan <- 1
						//fmt.Println(rf.me, " reset chan in StartElection, in term ", rf.currentTerm)
					} else {
						//fmt.Println(len(rf.peers), " servers in term ", rf.currentTerm)
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LeaderState
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
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
	// Your code here.
	// Example:
	println("data len: ",len(data))
	if data == nil || len(data) == 0{
		fmt.Println("empty data")
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	//fmt.Print("before change: currentTerm: ",rf.currentTerm," votedFor: ",rf.votedFor," log: ")
	//for _,val := range rf.log {
	//	fmt.Print(val.Term,",",val.Command," ")
	//}
	//fmt.Println()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	//fmt.Print(rf.me," readPersist: currentTerm: ",rf.currentTerm," votedFor: ",rf.votedFor," log: ")
	//for _,val := range rf.log {
	//	fmt.Print(val.Term,",",val.Command," ")
	//}
	//fmt.Println()
}

//
// example RequestVote RPC arguments structure.
//

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term > rf.currentTerm {
		if rf.state == LeaderState || rf.state == CandidateState {
			rf.BecomeFollower(args.Term)
		}
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}
	if args.LastLogTerm > rf.log[rf.getlastindex()].Term || (args.LastLogTerm == rf.log[rf.getlastindex()].Term && args.LastLogIndex >= rf.getlastindex()) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		//rf.ElectionTimer.Reset(rf.ElecTimeout)
		//fmt.Println(rf.me, " reset chan in RequestVote, in term ", rf.currentTerm)
		rf.resetChan <- 1
	}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) getlastindex() int{
	return len(rf.log) - 1
}

func getmin(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		//fmt.Println(args.Term, " < ", rf.currentTerm)
		return
	}
	if rf.state == LeaderState || rf.state == CandidateState {
		rf.BecomeFollower(args.Term)
	}
	//rf.print_args_entries(args.PrevLogIndex,args.Entries)
	if args.PrevLogIndex > rf.getlastindex(){
		reply.Success = false
	} else {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
		} else {
			reply.Success = true
			rf.log = rf.log[0 : args.PrevLogIndex + 1]     //remove conflicting entries
			change := false
			for _,tmpEntry := range args.Entries {
				rf.log = append(rf.log, tmpEntry)
				change = true
			}
			if(change){
				//rf.print_entries()
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = getmin(args.LeaderCommit, rf.getlastindex())
				rf.ApplyCmd()
			}
			rf.persist()
		}
	}
	//fmt.Println(rf.me, " reset chan in AppendEntries, in term ", rf.currentTerm)
	rf.resetChan <- 1
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == LeaderState
	if isLeader == false {
		return index, term, isLeader
	}
	index = rf.getlastindex() + 1
	term = rf.currentTerm
	NewEntry := LogEntry{Term: term, Command: command}
	rf.log = append(rf.log, NewEntry)
	rf.matchIndex[rf.me] = index
	rf.persist()
	//fmt.Println("Client sends command to ", rf.me)
	//rf.print_entries()
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
	// Your initialization code here.
	rf.applyCh = applyCh
	rf.RFInit()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//rf.BecomeFollower(0)
	rf.GenericRoutine()
	return rf
}
