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
	//"labgob"
	"math"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
import "../labgob"

const (
	Follower = iota
	Candidate
	Leader
	ResetTimer
	FlushState
)


const HeartBeatTimeout  = time.Duration(100) * time.Millisecond

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

type logEntries struct {
	Term int
	//Index int
	Command interface{}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	LastLogTerm int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	PreLogTerm int
	PrevLogIndex int
	Entries[] logEntries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	ConflictTerm int
	ConflictIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister    // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor int
	log []logEntries

	state int
	flushCh  chan int
	majority int32

	commitIndex  int
	lastApplied  int

	nextIndex   []int
	matchIndx   []int

	// tester commit channel
	applyCh     chan ApplyMsg
}



//helper functions
func (rf *Raft) beFollower(Term int)  {
	rf.state = Follower
	rf.currentTerm = Term
	rf.votedFor = -1//reset
	//DPrintf("%d convert to follower",rf.me)
	rf.persist()
}


func (rf *Raft) beCandidate()  {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	rf.persist()
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLogIdx(),
		LastLogTerm: rf.getLastLogTerm(),
	}

	go rf.kickoffElection(&args)
}


func (rf *Raft) beLeader()  {
	//leader must from candidate
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	rf.nextIndex = make([]int,len(rf.peers))
	rf.matchIndx = make([]int, len(rf.peers))
	for i := 0;i < len(rf.nextIndex);i++  {
		rf.nextIndex[i] = len(rf.log)
	}
}

//case 1:receive AppendEntries RPC from current leader or granting vote to candidate,reset timer
//case 2:step into leader and start heartbeat,jump out of select section in ticker() to flush leader state
func (rf *Raft) flush(behaviour int) {
	/*select {
	case <- rf.flushCh:
	default:
	}*/
	rf.flushCh <- behaviour
	//DPrintf("%d reset",rf.me)
}

//??????????????????????????????????????????
func (rf *Raft ) getLastLogIdx() int{
	return len(rf.log) - 1
}

func (rf *Raft ) getLastLogTerm() int{
	idx := rf.getLastLogIdx()
	if idx < 0 {
		return -1
	}
	return rf.log[idx].Term
}


//??????2??????????????????leader ??????leader??????nextindex - 1???????????????????????????????????????
func (rf *Raft ) getPrevLogIdx(idx int) int {
	return rf.nextIndex[idx] - 1
}

func (rf *Raft ) getPrevLogTerm(idx int) int {
	index := rf.getPrevLogIdx(idx)
	if index < 0{
		return -1
	}
	return rf.log[index].Term
}

//?????????(?????????)?????????server ??????????????????????????????????????????
func (rf *Raft)applyMessage(){
	//1. ???????????? ????????????apply
	for rf.lastApplied < rf.commitIndex{
		rf.lastApplied++
		msg := ApplyMsg{
			true,
			rf.log[rf.lastApplied].Command,
			rf.lastApplied,
		}
		rf.applyCh <- msg
	}
}

//leader ?????? ?????????currentTerm?????????
//If there exists an x such that x > commitIndex
//a majority of matchIndex[i] ??? x, and log[x].term == currentTerm: set commitIndex = x
func (rf *Raft) checkComit(server int) {
	x := rf.matchIndx[server]
	if x > rf.commitIndex{
		var cnt int32 = 0 //rf.majority is int32
		for _,m := range rf.matchIndx{
			if m >= x{
				cnt++
			}
		}
		//??????term????????? ?????????????????? ???????????????(d)??????????????? ??????????????????????????? ????????????????????????raft??????
		if cnt > rf.majority && rf.log[x].Term == rf.currentTerm{
			rf.commitIndex = x
			rf.applyMessage()
		}
	}
}





// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader =  rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//e.Encode(rf.state)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)

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
	// Your code here (2C).
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
	var votedFor	int
	var log			[]logEntries
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		//error
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.mu.Unlock()
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwiseAppendEntriesArgs
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
	// peers
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term{
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term{
		rf.beFollower(args.Term)
	}
	//????????????
	if rf.getLastLogTerm() > args.LastLogTerm ||
		(rf.getLastLogTerm() == args.LastLogTerm && rf.getLastLogIdx() > args.LastLogIndex){
		return
	}
	// If votedFor is null or candidateId, and candidate???s log is at
	// least as up-to-date as receiver???s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.state = Follower

		rf.persist()

		rf.flush(ResetTimer)
		// rf.persist()
	}
}


func (rf *Raft) kickoffElection(args *RequestVoteArgs){
	var voteCnt int32 = 1
	for pid := range rf.peers{
		if pid != rf.me{
			go func(idx int) {
				reply := &RequestVoteReply{}
				ret := rf.sendRequestVote(idx,args,reply)
				if ret {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm{
						rf.beFollower(reply.Term)
						return
					}
					if rf.state != Candidate || rf.currentTerm != args.Term{
						return
					}
					if reply.VoteGranted{
						atomic.AddInt32(&voteCnt,1)
						if atomic.LoadInt32(&voteCnt) > rf.majority{
							rf.beLeader()
							rf.flush(FlushState)
						}
					}
				}
			}(pid)
		}
	}
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = 0
	reply.ConflictTerm = -1
	if rf.currentTerm > args.Term{
		//DPrintf("%d refuse AE from %d",rf.me,args.LeaderId)
		return
	}
	if rf.currentTerm < args.Term{
		rf.beFollower(args.Term)
	}

	//???????????????term ??????index
	tmpPrevTerm := -1
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log){
		tmpPrevTerm = rf.log[args.PrevLogIndex].Term
	}
	//term?????? ?????????
	if tmpPrevTerm != args.PreLogTerm{
		//???????????????rf.log????????? ???len(rf.log)??????args.index??? ???????????????????????? ?????????????????????
		reply.ConflictIndex = len(rf.log)
		if tmpPrevTerm!=-1{
			reply.ConflictTerm = tmpPrevTerm
			//?????????????????????
			for i:= 0; i < len(rf.log); i++ {
				if rf.log[i].Term == tmpPrevTerm{
					reply.ConflictIndex = i
					break
				}
			}
		}
		rf.flush(ResetTimer)
		return
	}

	/*?????????  ??????  ?????????nextInx?????????????????????log????????????
	nextInx := args.PrevLogIndex + 1
	if nextInx == len(rf.log){
		rf.log = append(rf.log,args.Entries...)
	} else if nextInx < len(rf.log) {
		for i := 0; i < len(args.Entries); i++ {
			if nextInx < len(rf.log) && args.Entries[i].Term == rf.log[nextInx].Term {
				nextInx++
				continue
			}
			rf.log = rf.log[:nextInx]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	*/

	//nextIndex match this server,start replicate log
	index := args.PrevLogIndex
	for i:=0 ;i < len(args.Entries);i++ {
		index++
		if index < len(rf.log) {
			if rf.log[index].Term == args.Entries[i].Term {
				continue
			} else {
				//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
				//The if here is crucial. If the follower has all the entries the leader sent, the follower MUST NOT truncate its log.
				//Any elements following the entries sent by the leader MUST be kept. This is because we could be receiving an outdated AppendEntries RPC from the leader,
				//and truncating the log would mean ???taking back??? entries that we may have already told the leader that we have in our log.
				rf.log = rf.log[:index]
			}
		}
		rf.log = append(rf.log,args.Entries[i:]...)
		rf.persist()
		break
	}

	rf.persist()

	//???????????????commit
	if args.LeaderCommit > rf.commitIndex{
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit),float64(rf.getLastLogIdx())))
		rf.applyMessage()
	}
	reply.Success = true
	rf.flush(ResetTimer)

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// peers
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//DPrintf("%d send AE to %d",rf.me,server)
	return ok
}

func (rf *Raft) groupAppendLog(){
	/* args????????????goroutine?????? ?????????????????????server ??????????????????????????????
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		rf.currentTerm,
		rf.me,

	}
	rf.mu.Unlock()*/
	for pid := range rf.peers{
		if pid != rf.me{
			go func(idx int) {
				for {
					rf.mu.Lock()
					if rf.state != Leader {
						rf.mu.Unlock()
						return
					}
					args := &AppendEntriesArgs{
						rf.currentTerm,
						rf.me,
						rf.getPrevLogTerm(idx),
						rf.getPrevLogIdx(idx),
						//rf.log[rf.nextIndex[idx]:],
						append(make([]logEntries,0),rf.log[rf.nextIndex[idx]:]...),
						rf.commitIndex,
					}
					//should not holding the lock while calling RPC   timeout?
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(idx, args, reply)
					rf.mu.Lock()
					//??????????????????leader state??????
					if !ok || rf.state != Leader || rf.currentTerm != args.Term{
						rf.mu.Unlock()
						return
					}
					//
					if args.Term < reply.Term {
						rf.beFollower(reply.Term)
						rf.mu.Unlock()
						return
					}
					//?????? ?????????leader??????????????? matchIndex nextIndex checkCommit
					if reply.Success{
						//?????????rf.matchIndx[idx] += len(args.Entries)???  ???????????????matchIndx????????????
						//args.PrevLogIndex ?????????????????????????????????
						rf.matchIndx[idx] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[idx] = rf.matchIndx[idx] + 1
						//?????????????????????
						rf.checkComit(idx)
						rf.mu.Unlock()
						return
					}else{//??????
						changeNextIndex := reply.ConflictIndex
						if reply.ConflictTerm != -1 {
							//leader??????????????????????????? ???????????????????????????????????????????
							for i:= 0; i < len(rf.log); i++{
								if rf.log[i].Term == reply.ConflictTerm{
									for i< len(rf.log) && rf.log[i].Term == reply.ConflictTerm{
										i++
									}
									changeNextIndex = i
									break
								}
							}
						}
						rf.nextIndex[idx] = changeNextIndex
						rf.mu.Unlock()
						//should not return here,wait for next reply
						//?????????????????? ??????????????????????????????????????????
					}
				}
			}(pid)
		}
	}
}


func (rf *Raft) ticker(){
	//????????? select???????????????????????? flushCh????????? ??????????????????????????????????????? ??????????????????rand???
	for !rf.killed(){
		//3-5 heart_eat 100ms
		electionTimeout :=  rand.Intn(200) + 300

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower, Candidate:
			select {
			case <- time.After(time.Duration(electionTimeout) * time.Millisecond):
				//out of time,kick off election
				rf.mu.Lock()
				rf.beCandidate()
				rf.mu.Unlock()
			case <- rf.flushCh:
				//case 1:receive heartBeat or replicated log or vote for candidate,reset timer
				//case 2:be leader and start heartbeat,jump out of select to flush state
			}
		case Leader:
			rf.groupAppendLog()          //??????
			time.Sleep(HeartBeatTimeout) //Sleep HeartBeatTimeout
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
//start function is used to append new command to leader index???command???????????????????????????
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	// Your code here (2B).

	if isLeader{
		index = rf.getLastLogIdx()+1
		Entry := logEntries{
			Term: rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log,Entry)

		rf.persist()

		rf.matchIndx[rf.me] = rf.getLastLogIdx()//????????????++ ??????append???command??????????????????
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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


	// Your initialization code here (2A, 2B, 2C).
	rf.majority = int32(len(rf.peers)/2)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.flushCh = make(chan int,1)
	rf.log = make([]logEntries,1)
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	return rf
}

