package raft

const (
	FollowerState = iota
	CandidateState
	LeaderState
)

type LogEntry struct {
	term    int
	command interface{}
}

type RequestVoteArgs struct {
	// Your data here.
	Term        int
	CandidateId int
	//LastLogIndex int32
	//LastLogTerm  int32
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}
