package backup

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
	"log"
	"math"
	"math/rand"
	"mit_ds_2021/raft"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"mit_ds_2021/labrpc"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *raft.Persister     // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	VoteFor     int
	Logs        []*Log
	CommitIndex int
	LastApplied int
	RaftStatus  int
	LastIndex   int
	LastTerm    int

	//leader
	NextIndex  []int
	MatchIndex []int

	//other
	HeartSignal chan AppendEntries
	//ElectionSignal     chan RequestVote
	LeaderId          int
	LastActiveTime    time.Time
	LastBroadcastTime time.Time
}

type Log struct {
	LogIndex    int
	CreateTerm  int
	Command     interface{}
	IsCommitted bool
	IsApplied   bool
}

type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Log
	LeaderCommit int
}

type LeaderClaimArgs struct {
	AppendEntries
	VoteGrantedNum int
}

type AppendEntriesReply struct {
	Term     int
	LeaderId int
	Success  bool
}

// AppendEntries todo 添加日志条目
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		return
	}
	//发现更大term，更新信息转换follower
	if rf.CurrentTerm < args.Term {
		rf.RaftStatus = FOLLOWER
		rf.CurrentTerm = args.Term
	}
	//保存新的leader
	rf.LeaderId = args.LeaderId
	//更新活跃时间
	rf.LastActiveTime = time.Now()
	log.Printf("leader alive...")
	//保存leader发来的日志条目
	lastIndex := rf.lastIndex()
	rf.mu.Unlock()
	for i, entry := range args.Entries {
		rf.mu.Lock()
		index := lastIndex + i + 1
		if index <= entry.LogIndex {
			rf.Logs = append(rf.Logs, entry)
		} else {
			if entry.CreateTerm != rf.Logs[index-1].CreateTerm {
				rf.Logs = rf.Logs[:index-1]
				rf.Logs = append(rf.Logs, entry)
			}
		}
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	//检查leader是否有更新的提交，如果有，那么在本地状态机应用并更新提交
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = args.LeaderCommit
		if lastIndex < args.LeaderCommit {
			rf.CommitIndex = lastIndex
		}
	}
	args.PrevLogTerm = rf.lastTerm()
	args.PrevLogIndex = rf.lastIndex()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.RLock()
	term = rf.CurrentTerm
	isleader = rf.RaftStatus == LEADER
	log.Printf("server %d status is %d...", rf.me, rf.RaftStatus)
	rf.mu.RUnlock()
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVote
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVote struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVote, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//term比我小，拒绝投票，并将新数据放入reply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	//发现更大term，更新term，转换follower，并投票
	if args.Term > rf.CurrentTerm {
		log.Printf("server %d expired, new term is %d from %d", rf.me, args.Term, args.CandidateId)
		rf.CurrentTerm = args.Term
		rf.RaftStatus = FOLLOWER
		rf.VoteFor = args.CandidateId
		reply.VoteGranted = true
		rf.LastActiveTime = time.Now()
		log.Printf("server %d vote to %d", rf.me, args.CandidateId)
		return
	}
	//投票，并设置只能投给一个人
	if rf.VoteFor == -1 || rf.VoteFor == args.CandidateId {
		//todo 检查最后一条日志的term和日志列表长度以后再决定是否投票
		if rf.Logs[rf.lastIndex()].CreateTerm <= args.LastLogTerm {
			rf.VoteFor = args.CandidateId
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = true
		}
		rf.LastActiveTime = time.Now()
		log.Printf("server %d vote to %d", rf.me, args.CandidateId)
		return
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVote, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	//Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//如果不是leader，返回false
	if rf.RaftStatus != LEADER {
		return -1, -1, false
	}
	//构建log，并加入到leader的log_list中
	rf.Logs = append(rf.Logs, &Log{
		LogIndex:    rf.lastIndex() + 1,
		CreateTerm:  rf.CurrentTerm,
		Command:     command,
		IsCommitted: false,
		IsApplied:   false,
	})
	log.Printf("log %b update", command)
	index = rf.lastIndex()
	term = rf.CurrentTerm
	return index, term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) lastIndex() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if len(rf.Logs) != 0 {
		return rf.Logs[len(rf.Logs)-1].LogIndex
	} else {
		return -1
	}
}

func (rf *Raft) lastTerm() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if len(rf.Logs) != 0 {
		return rf.Logs[len(rf.Logs)-1].CreateTerm
	} else {
		return -1
	}
}

//todo 执行日志复制与心跳
func (rf *Raft) doAppendEntries(peerId int) {
	rf.mu.RLock()
	request := AppendEntries{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.lastIndex(),
		PrevLogTerm:  rf.lastTerm(),
		Entries:      rf.Logs,
		LeaderCommit: rf.CommitIndex,
	}
	rf.mu.RUnlock()
	go func(peer int) {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, &request, &reply)
		if ok {
			rf.mu.Lock()
			//检查是否中间选举过
			if rf.CurrentTerm != request.Term {
				rf.mu.Unlock()
				return
			}
			//检查返回的term，是否该leader过期
			if reply.Term > rf.CurrentTerm {
				rf.RaftStatus = FOLLOWER
				rf.CurrentTerm = reply.Term
				rf.VoteFor = -1
				rf.LeaderId = -1
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				//日志同步成功，leader需要更新nextIndex数组和matchIndex数组
				//nextIndex使用args里面的prevLogIndex更新（prevLogIndex直接在args中更新？）
				rf.NextIndex[peerId] = request.PrevLogIndex + 1
				//matchIndex使用nextIndex更新，他就是与leader匹配的那个index
				rf.MatchIndex[peerId] = request.PrevLogIndex
			} else {
				//todo 日志同步失败
			}
			rf.mu.Unlock()
		}
	}(peerId)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) appendLogSignal() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		func() {
			rf.mu.Lock()
			//判断状态，leader才能发送
			if rf.RaftStatus != LEADER {
				rf.mu.Unlock()
				return
			}
			//设置计时器，隔100ms发送一次
			now := time.Now()
			if now.Sub(rf.LastBroadcastTime) < time.Millisecond*100 {
				rf.mu.Unlock()
				return
			}
			rf.LastBroadcastTime = now
			me := rf.me
			request := AppendEntries{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.lastIndex(),
				PrevLogTerm:  rf.lastTerm(),
				Entries:      rf.Logs,
				LeaderCommit: rf.CommitIndex,
			}
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i == me {
					continue
				}
				//rf.doAppendEntries(i)
				rf.sendAppendEntries(i, &request, &AppendEntriesReply{})
			}
		}()
	}
}

//todo 日志条目应用进程
func (rf *Raft) applyLog() {

}

func (rf *Raft) election() {
	back := 0
	for rf.killed() == false {
		back++
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//设置随机选举超时(超过这个时间才选举)
			now := time.Now()
			timeout := time.Duration(rand.Intn(500*back)+300) * time.Millisecond
			interval := now.Sub(rf.LastActiveTime)
			//状态转换
			if rf.RaftStatus == FOLLOWER {
				if interval >= timeout {
					rf.RaftStatus = CANDIDATE
				}
			}
			//选举逻辑
			if rf.RaftStatus == CANDIDATE && interval >= timeout {
				//字段初始化
				rf.VoteFor = rf.me
				rf.CurrentTerm++
				rf.LastActiveTime = now
				//构造投票请求，并发拉票
				requestVote := RequestVote{
					Term:         rf.CurrentTerm,
					CandidateId:  rf.me,
					LastLogIndex: -1,
					LastLogTerm:  -1,
				}
				maxTerm := rf.CurrentTerm
				me := rf.me
				rf.mu.Unlock()
				type VoteResult struct {
					peerId int
					reply  *RequestVoteReply
				}
				voteNum := 1
				replyNum := 1
				voteResults := make(chan VoteResult)
				for i := 0; i < len(rf.peers); i++ {
					go func(peerId int) {
						if peerId == me {
							return
						}
						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(peerId, &requestVote, &reply)
						if ok {
							voteResults <- VoteResult{peerId: peerId, reply: &reply}
						} else {
							voteResults <- VoteResult{peerId: peerId, reply: nil}
						}
					}(i)
				}
				//检测投票情况，完成拉票或者已经得到大多数选票后退出
				for {
					select {
					case voteResult := <-voteResults:
						replyNum++
						if voteResult.reply != nil {
							if voteResult.reply.VoteGranted {
								voteNum++
							}
							if voteResult.reply.Term > maxTerm {
								maxTerm = voteResult.reply.Term
							}
						}
						if replyNum == len(rf.peers) {
							goto AFTER_VOTE
						}
					default:

					}
				}
			AFTER_VOTE:
				rf.mu.Lock()
				if rf.RaftStatus != CANDIDATE {
					return
				}
				//term变大，更新term，转换follower并return
				if rf.CurrentTerm < maxTerm {
					log.Printf("election: server %d exit election because expired...", rf.me)
					rf.CurrentTerm = maxTerm
					rf.RaftStatus = FOLLOWER
					rf.VoteFor = -1
					rf.LeaderId = -1
					return
				}
				//获取大多数选票，转换leader并return
				if float64(voteNum) >= math.Ceil(float64(len(rf.peers))/2) {
					log.Printf("election: server %d be elected leader...", rf.me)
					rf.RaftStatus = LEADER
					rf.LeaderId = rf.me
					rf.LastBroadcastTime = time.Unix(0, 0)
					return
				} else {
					rf.RaftStatus = FOLLOWER
					return
				}
			}
		}()
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *raft.Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.RaftStatus = FOLLOWER
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.Logs = []*Log{}
	rf.CommitIndex = -1
	rf.LastApplied = -1
	rf.HeartSignal = make(chan AppendEntries)
	rf.LeaderId = -1
	//rf.ElectionSignal = make(chan RequestVote)
	//rf.ElectionFailSignal = make(chan int)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.election()
	go rf.appendLogSignal()
	return rf
}
