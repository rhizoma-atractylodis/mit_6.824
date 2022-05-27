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
	"log"
	"math/rand"
	"mit_ds_2021/labgob"
	"os"
	"sort"

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

func init() {
	logFile, err := os.OpenFile("./log.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("open log file failed, err:", err)
		return
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Llongfile | log.Lmicroseconds | log.Ldate)
}

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
	mu                sync.RWMutex        // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	CurrentTerm       int
	VoteFor           int
	Logs              []Log
	CommitIndex       int
	LastApplied       int
	RaftStatus        int
	NextIndex         []int
	MatchIndex        []int
	LogSignal         chan bool
	LeaderId          int
	LastActiveTime    time.Time
	LastBroadcastTime time.Time
	ApplyLogSignal    chan ApplyMsg
	CurrentServer     map[int]bool
	LastIncludedIndex int
	LastIncludedTerm  int
}

type Log struct {
	LogIndex    int
	CreateTerm  int
	Command     interface{}
	IsCommitted bool
	IsApplied   bool
}

func (l Log) String() string {
	return fmt.Sprintf("{index: %d, trem: %d, command: %v, isCommitted: %v, isApplied: %v}",
		l.LogIndex, l.CreateTerm, l.Command, l.IsCommitted, l.IsApplied)
}

type AppendEntries struct {
	Term          int
	LeaderId      int
	PrevLogIndex  int
	PrevLogTerm   int
	Entries       []Log
	LeaderCommit  int
	CurrentServer map[int]bool
}

type AppendEntriesReply struct {
	Term          int
	LeaderId      int
	Success       bool
	Alive         bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// AppendEntries 添加日志条目
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		rf.CurrentServer[i] = args.CurrentServer[i]
	}
	rf.CurrentServer[rf.me] = true
	//发现更大term，更新信息转换follower
	if rf.CurrentTerm < args.Term {
		rf.RaftStatus = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.persist()
	}
	if rf.CurrentTerm > args.Term {
		reply.Alive = true
		reply.Term = rf.CurrentTerm
		return
	}
	//保存新的leader
	rf.LeaderId = args.LeaderId
	//更新活跃时间
	rf.LastActiveTime = time.Now()
	reply.Alive = true
	//log.Printf("leader %d current commit is %v, server commit %d", args.LeaderId, args.LeaderCommit, rf.CommitIndex)
	//todo 日志复制时考虑快照的影响
	//日志冲突检测
	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.ConflictIndex = 1
		return
	} else if args.PrevLogIndex == rf.LastIncludedIndex {
		if args.PrevLogTerm != rf.LastIncludedTerm {
			reply.ConflictIndex = 1
			return
		}
	} else {
		if (len(rf.Logs) > 0) && (args.PrevLogIndex != 0) {
			if args.PrevLogIndex > lastIndex(rf.Logs) {
				reply.ConflictIndex = lastIndex(rf.Logs) + 1
				return
			}
			if rf.Logs[rf.locateLogByIndex(args.PrevLogIndex)].CreateTerm != args.PrevLogTerm {
				reply.ConflictTerm = rf.Logs[rf.locateLogByIndex(args.PrevLogIndex)].CreateTerm
				for i := rf.Logs[0].LogIndex; i < args.PrevLogIndex+1; i++ {
					if rf.Logs[rf.locateLogByIndex(i)].CreateTerm == reply.ConflictTerm {
						reply.ConflictIndex = i + 1
						break
					}
				}
				return
			}
		}
	}
	//日志复制
	//log.Printf("entries: %v", args.Entries)
	if len(rf.Logs) == 0 {
		rf.Logs = args.Entries
	} else {
		for _, entry := range args.Entries {
			index := lastIndex(rf.Logs)
			if index < entry.LogIndex {
				rf.Logs = append(rf.Logs, entry)
				log.Printf("follower %d: append log %v -> %v from leader %d", rf.me, entry, rf.Logs, args.LeaderId)
			} else {
				if lastTerm(rf.Logs) != entry.CreateTerm {
					rf.Logs = rf.Logs[:rf.locateLogByIndex(entry.LogIndex)]
					rf.Logs = append(rf.Logs, entry)
					log.Printf("follower %d: overwrite log %v -> %v from leader %d", rf.me, entry, rf.Logs, args.LeaderId)
				}
			}
		}
	}
	//检查leader是否有更新的提交，如果有，那么在本地状态机应用并更新提交
	if args.LeaderCommit > rf.CommitIndex {
		//log.Printf("follower %d: commit from %d to %d", rf.me, rf.CommitIndex, args.LeaderCommit)
		commit := args.LeaderCommit
		if lastIndex(rf.Logs) < args.LeaderCommit {
			//log.Printf("follower %d: commit from %d to %d", rf.me, rf.CommitIndex, lastIndex(rf.Logs))
			commit = lastIndex(rf.Logs)
		}
		//log.Printf("follower %d commit %v", rf.me, rf.Logs[rf.CommitIndex-1])
		for i := rf.CommitIndex + 1; i <= commit; i++ {
			rf.Logs[rf.locateLogByIndex(i)].IsCommitted = true
		}
		rf.CommitIndex = commit
	}
	rf.persist()
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
	//log.Printf("server %d status is %d...", rf.me, rf.RaftStatus)
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
	rf.persister.SaveRaftState(rf.raftStatusForPersist())
}

func (rf *Raft) raftStatusForPersist() []byte {
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	_ = encoder.Encode(rf.Logs)
	_ = encoder.Encode(rf.VoteFor)
	_ = encoder.Encode(rf.CurrentTerm)
	return w.Bytes()
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
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	_ = decoder.Decode(&rf.Logs)
	_ = decoder.Decode(&rf.VoteFor)
	_ = decoder.Decode(&rf.CurrentTerm)
}

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//todo 判断日志是否可以应用到状态机
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	//if rf.LastIncludedIndex > lastIncludedIndex {
	//	return false
	//} else if rf.LastIncludedIndex == lastIncludedIndex {
	//	return true
	//} else {
	//	//todo 丢弃部分日志
	//	return true
	//}
	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//todo 创建快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//todo 判断是否有index更大的快照
	if rf.LastIncludedIndex >= index {
		return
	}
	//todo 计算需要压缩的日志大小
	compactSize := index - rf.LastIncludedIndex
	rf.LastIncludedTerm = rf.Logs[rf.locateLogByIndex(index)].CreateTerm
	rf.LastIncludedIndex = index
	//todo make一个新的log切片，copy非压缩区日志到新log切片
	logs := make([]Log, len(rf.Logs)-compactSize, len(rf.Logs)-compactSize)
	copy(logs, rf.Logs[rf.locateLogByIndex(index+1):])
	//todo 更新raft存储的日志，将压缩过的丢弃
	rf.Logs = logs
	//todo 调用持久化方法将snapshot持久化
	rf.persister.SaveStateAndSnapshot(rf.raftStatusForPersist(), snapshot)
}

// InstallSnapshot
//todo 作为follower与leader的快照同步(只有当该follower严重落后的情况)
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//todo 如果请求方term更小，则直接拒绝什么也不做
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	//todo 如果收到了更大的term请求，则转换为follower，并清除状态
	if args.Term > rf.CurrentTerm {
		rf.RaftStatus = FOLLOWER
		rf.VoteFor = -1
		rf.CurrentTerm = args.Term
		rf.persist()
	}
	//todo 记忆新的leader并更新活跃时间
	rf.LeaderId = args.LeaderId
	rf.LastActiveTime = time.Now()
	//todo 如果该快照比本地的更小，那么拒绝这个快照
	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		return
	} else {
		//todo 否则就需要保留快照，这时需要判断这个快照能否覆盖本机其他日志，如果能覆盖，直接丢弃本机所有的日志
		if args.LastIncludedIndex < lastIndex(rf.Logs) {
			//todo 如果无法全部覆盖，就需要判断截断日志的方法。比较快照最后一个条目的term，如果与第一个不能被覆盖的日志冲突，那么丢弃本机所有日志
			if args.LastIncludedTerm != rf.Logs[rf.locateLogByIndex(args.LastIncludedIndex)].CreateTerm {
				rf.Logs = make([]Log, 0)
			} else {
				//todo 如果没有冲突，那就保留这些日志
				logs := make([]Log, lastIndex(rf.Logs)-args.LastIncludedIndex)
				copy(logs, rf.Logs[rf.locateLogByIndex(args.LastIncludedIndex+1):])
				rf.Logs = logs
			}
		} else {
			rf.Logs = make([]Log, 0)
		}
	}
	//todo 更新raft快照元数据
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	//todo 持久化并提交到应用层
	rf.persister.SaveStateAndSnapshot(rf.raftStatusForPersist(), args.Data)
	rf.applySnapshot(args.Data)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) doInstallSnapshot(peerId int) {
	//InstallSnapshotArgs作为rpc请求信息
	request := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.LeaderId,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
		Offset:            0,
		Done:              true,
	}
	reply := InstallSnapshotReply{}
	//todo 后续发送处理与日志复制部分类似
	go func(id int) {
		ok := rf.sendInstallSnapshot(id, &request, &reply)
		if ok {
			rf.mu.Lock()
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
				rf.persist()
				return
			}
			rf.MatchIndex[id] = rf.LastIncludedIndex
			rf.NextIndex[id] = rf.LastIncludedIndex + 1
			rf.updateCommit()
			rf.mu.Unlock()
		}
	}(peerId)
}

func (rf *Raft) applySnapshot(snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//todo 拼接ApplyMsg对象
	applyMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotIndex: rf.LastIncludedIndex,
		SnapshotTerm:  rf.LastIncludedTerm,
	}
	//todo 放入applyCh管道，提交到应用层
	rf.ApplyLogSignal <- applyMsg
	rf.LastApplied = rf.LastIncludedIndex
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
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	//发现更大term，更新term，转换follower，并投票
	if args.Term > rf.CurrentTerm {
		//log.Printf("server %d expired, new term is %d from %d", rf.me, args.Term, args.CandidateId)
		rf.RaftStatus = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VoteFor = -1
	}
	//投票，并设置只能投给一个人
	if rf.VoteFor == -1 || rf.VoteFor == args.CandidateId {
		term := lastTerm(rf.Logs)
		if (term < args.LastLogTerm) || ((term == args.LastLogTerm) && (lastIndex(rf.Logs) <= args.LastLogIndex)) {
			rf.VoteFor = args.CandidateId
			reply.Term = rf.CurrentTerm
			rf.RaftStatus = FOLLOWER
			reply.VoteGranted = true
			rf.LastActiveTime = time.Now()
		}
		//log.Printf("election: server %d vote to %d", rf.me, args.CandidateId)
	}
	rf.mu.Unlock()
	rf.persist()
}

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

	// Your code here (2B).
	rf.mu.Lock()
	last := lastIndex(rf.Logs)
	//如果不是leader，返回false
	if rf.RaftStatus != LEADER {
		rf.mu.Unlock()
		return -1, -1, false
	}
	//构建log，并加入到leader的log_list中
	rf.Logs = append(rf.Logs, Log{
		LogIndex:    last + 1,
		CreateTerm:  rf.CurrentTerm,
		Command:     command,
		IsCommitted: false,
		IsApplied:   false,
	})
	rf.NextIndex[rf.me] += 1
	rf.MatchIndex[rf.me] += 1
	term = rf.CurrentTerm
	index = lastIndex(rf.Logs)
	rf.mu.Unlock()
	rf.persist()
	log.Printf("leader %d insert %v to log, index is %d -> %v", rf.me, command, index, rf.Logs)
	return index, term, true
}

// Kill
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

func aliveServer(currentServer map[int]bool) int {
	alive := 0
	for _, isAlive := range currentServer {
		if isAlive {
			alive += 1
		}
	}
	return alive
}

func (rf *Raft) updateCommit() {
	//match 超过半数与leader index一致就commit
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//count, alive := 0, aliveServer(rf.CurrentServer)
	//for _, index := range rf.MatchIndex {
	//	if index == rf.MatchIndex[rf.me] {
	//		count += 1
	//	}
	//}
	////log.Printf("leader %d current alive %d server, %v, %v", rf.me, alive, rf.CurrentServer, rf.MatchIndex)
	//if count > alive/2 {
	//	preCommit := rf.CommitIndex + 1
	//	rf.CommitIndex = rf.MatchIndex[rf.me]
	//	for i := preCommit; i <= rf.CommitIndex; i++ {
	//		log.Printf("leader %d commit %v", rf.me, rf.Logs[rf.locateLogByIndex(i)])
	//		rf.Logs[rf.locateLogByIndex(i)].IsCommitted = true
	//	}
	//}
	sort.Ints(rf.MatchIndex)
	log.Printf("match: %v", rf.MatchIndex)
	median := rf.MatchIndex[len(rf.peers)/2]
	if median > rf.CommitIndex && rf.Logs[rf.locateLogByIndex(median)].CreateTerm == rf.CurrentTerm {
		preCommit := rf.CommitIndex + 1
		rf.CommitIndex = median
		for i := preCommit; i <= rf.CommitIndex; i++ {
			log.Printf("leader %d commit %v", rf.me, rf.Logs[rf.locateLogByIndex(i)])
			rf.Logs[rf.locateLogByIndex(i)].IsCommitted = true
		}
	}
}

//todo 添加快照支持
func (rf *Raft) doAppendEntries(peerId int) {
	go func(peer int) {
		rf.mu.Lock()
		request := AppendEntries{
			Term:          rf.CurrentTerm,
			LeaderId:      rf.me,
			LeaderCommit:  rf.CommitIndex,
			CurrentServer: rf.CurrentServer,
			PrevLogIndex:  rf.NextIndex[peer] - 1,
			PrevLogTerm:   lastTerm(rf.Logs),
			Entries:       rf.Logs,
		}
		//todo log为空的处理
		if request.PrevLogIndex <= 0 {
			request.Entries = rf.Logs
		} else {
			if request.PrevLogIndex < rf.LastIncludedIndex {
				//todo 如果pre在快照之前
				request.PrevLogTerm = rf.LastIncludedTerm
			} else if request.PrevLogIndex == rf.LastIncludedIndex {
				request.PrevLogTerm = rf.LastIncludedTerm
			} else {
				request.PrevLogTerm = rf.Logs[rf.locateLogByIndex(request.PrevLogIndex)].CreateTerm
			}
			request.Entries = rf.Logs[rf.locateLogByIndex(request.PrevLogIndex)+1:]
		}
		//log.Printf("next list: %v", rf.NextIndex)
		//log.Printf("server %d (pre index %d term %d) send log address: %v", peer, request.PrevLogIndex, request.PrevLogTerm, request.Entries)
		rf.mu.Unlock()
		reply := AppendEntriesReply{
			Alive:        false,
			Success:      false,
			ConflictTerm: -1,
		}
		ok := rf.sendAppendEntries(peer, &request, &reply)
		if ok {
			rf.mu.Lock()
			//检查是否中间选举过
			//rf.MatchIndex[rf.me] = lastIndex(request.Entries)
			if rf.CurrentTerm != request.Term {
				rf.RaftStatus = FOLLOWER
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
				rf.persist()
				return
			}
			rf.CurrentServer[peer] = reply.Alive
			//if len(request.Entries) != 0 {
			if reply.Success {
				//日志同步成功，leader需要更新nextIndex数组和matchIndex数组
				//nextIndex使用args里面的prevLogIndex更新（prevLogIndex直接在args中更新？）
				rf.NextIndex[peer] = request.PrevLogIndex + len(request.Entries) + 1
				//matchIndex使用nextIndex更新，他就是与leader匹配的那个index
				rf.MatchIndex[peer] = rf.NextIndex[peer] - 1
				//log.Printf("leader %d next index list: %v", rf.me, rf.NextIndex)
				//log.Printf("leader %d match index list: %v", rf.me, rf.MatchIndex)
				rf.updateCommit()
			} else {
				//todo 由于快照，需要更新
				//log.Printf("follower %d fail", peer)
				if reply.ConflictTerm != -1 {
					conflictIndex := -1
					//搜索conflictTerm对应的最大的index
					for i := request.PrevLogIndex; i > rf.MatchIndex[peer]; i-- {
						if rf.Logs[rf.locateLogByIndex(i)].CreateTerm == reply.ConflictTerm {
							conflictIndex = i
							break
						}
					}
					if conflictIndex == -1 {
						rf.NextIndex[peer] = reply.ConflictIndex
						//log.Printf("follower %d, next index %d", peer, rf.NextIndex[peer])
					} else {
						rf.NextIndex[peer] = conflictIndex
					}
				} else {
					rf.NextIndex[peer] = reply.ConflictIndex
				}
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			rf.CurrentServer[peer] = false
			rf.mu.Unlock()
		}
	}(peerId)
}

// The ticker go routine starts a new election if this peer hasn't received
// hearts beats recently.
func (rf *Raft) appendLogSignal() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Millisecond * 10)
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
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i != me {
					//if rf.NextIndex[i] <= rf.LastIncludedIndex {
					//	rf.doInstallSnapshot(i)
					//} else {
					//	rf.doAppendEntries(i)
					//}
					rf.doAppendEntries(i)
				}
			}
		}()
	}
}

func (rf *Raft) election() {
	back := 0
	for rf.killed() == false {
		time.Sleep(time.Millisecond * 10)
		back++
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//设置随机选举超时(超过这个时间才选举)
			now := time.Now()
			timeout := time.Duration(300+rand.Int31n(int32(500*back))) * time.Millisecond
			interval := now.Sub(rf.LastActiveTime)
			//状态转换
			if rf.RaftStatus == FOLLOWER {
				if interval >= timeout {
					rf.RaftStatus = CANDIDATE
				}
			}
			//选举逻辑
			if rf.RaftStatus == CANDIDATE && interval >= timeout {
				//log.Printf("election: server %d start election", rf.me)
				//字段初始化
				rf.VoteFor = rf.me
				rf.CurrentTerm++
				rf.persist()
				rf.LastActiveTime = now
				if rf.LeaderId != -1 {
					rf.CurrentServer[rf.LeaderId] = false
				}
				//构造投票请求，并发拉票
				requestVote := RequestVote{
					Term:         rf.CurrentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastIndex(rf.Logs),
					LastLogTerm:  lastTerm(rf.Logs),
				}
				maxTerm := rf.CurrentTerm
				me := rf.me
				serverNum := aliveServer(rf.CurrentServer)
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
						if replyNum == serverNum {
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
					//log.Printf("election: server %d exit election because expired...", rf.me)
					rf.CurrentTerm = maxTerm
					rf.RaftStatus = FOLLOWER
					rf.VoteFor = -1
					rf.LeaderId = -1
					rf.persist()
					return
				}
				//获取大多数选票，转换leader并return
				if voteNum > serverNum/2 {
					log.Printf("election: server %d be elected leader... log: %v", rf.me, rf.Logs)
					rf.RaftStatus = LEADER
					rf.LeaderId = rf.me
					rf.NextIndex = make([]int, len(rf.peers), len(rf.peers))
					rf.MatchIndex = make([]int, len(rf.peers), len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.NextIndex[i] = lastIndex(rf.Logs) + 1
						rf.MatchIndex[i] = 0
					}
					rf.LastBroadcastTime = time.Unix(0, 0)
					return
				} else {
					rf.RaftStatus = FOLLOWER
					rf.VoteFor = -1
					rf.LeaderId = -1
					rf.persist()
					return
				}
			}
		}()
	}
}

func (rf *Raft) applyLog() {
	for rf.killed() == false {
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//判断commit和apply index关系，如果commit index变大，则需要有应用的条目
			for rf.CommitIndex > rf.LastApplied {
				rf.LastApplied += 1
				l := rf.Logs[rf.locateLogByIndex(rf.LastApplied)]
				//如果需要提交，拼接ApplyMsg发送到applyLogSignal
				applyMsg := ApplyMsg{
					Command:       l.Command,
					CommandIndex:  l.LogIndex,
					CommandValid:  true,
					SnapshotValid: false,
				}
				rf.ApplyLogSignal <- applyMsg
				rf.Logs[rf.locateLogByIndex(rf.LastApplied)].IsApplied = true
				log.Printf("server %d apply log %v", rf.me, l)
			}
		}()
	}
}

func lastIndex(logs []Log) int {
	if len(logs) != 0 {
		return logs[len(logs)-1].LogIndex
	} else {
		return 0
	}
}

func lastTerm(logs []Log) int {
	if len(logs) != 0 {
		return logs[len(logs)-1].CreateTerm
	} else {
		return -1
	}
}

func (rf *Raft) locateLogByIndex(index int) int {
	return index - rf.LastIncludedIndex - 1
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.RaftStatus = FOLLOWER
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.Logs = []Log{}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.LogSignal = make(chan bool, len(rf.peers)-1)
	rf.LeaderId = -1
	rf.ApplyLogSignal = applyCh
	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = 1
	rf.CurrentServer = make(map[int]bool, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.CurrentServer[i] = true
	}
	//rf.applySnapshot([]byte{})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.election()
	go rf.appendLogSignal()
	go rf.applyLog()
	return rf
}
