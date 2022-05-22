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
	"fmt"
	"log"
	"math/rand"
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
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	VoteFor     int
	Logs        []Log
	CommitIndex int
	LastApplied int
	RaftStatus  int

	//leader
	NextIndex  []int
	MatchIndex []int

	//other
	LogSignal chan bool
	//ElectionSignal     chan RequestVote
	LeaderId          int
	LastActiveTime    time.Time
	LastBroadcastTime time.Time
	ApplyLogSignal    chan ApplyMsg
	CurrentServer     map[int]bool
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
	Term     int
	LeaderId int
	Success  bool
	Alive    bool
}

// AppendEntries 添加日志条目
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args.CurrentServer[rf.me] = true
	rf.CurrentServer = args.CurrentServer
	//发现更大term，更新信息转换follower
	if rf.CurrentTerm < args.Term {
		rf.RaftStatus = FOLLOWER
		rf.CurrentTerm = args.Term
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
	//log.Printf("server %d current log is %v", rf.me, rf.Logs)
	if len(args.Entries) != 0 {
		log.Printf("follower %d log %v", rf.me, args.Entries)
		if lastIndex(rf.Logs) == 0 {
			rf.Logs = args.Entries
		} else {
			rf.mu.Unlock()
			for _, entry := range args.Entries {
				rf.mu.Lock()
				index := lastIndex(rf.Logs)
				if index < entry.LogIndex {
					rf.Logs = append(rf.Logs, entry)
					log.Printf("follower %d: append log %v -> %v from leader %d", rf.me, entry, rf.Logs, args.LeaderId)
				} else {
					if rf.Logs[lastIndex(rf.Logs)-1].CreateTerm <= entry.CreateTerm {
						rf.Logs = rf.Logs[:index]
						rf.Logs = append(rf.Logs, entry)
						log.Printf("follower %d: overwrite log %v -> %v from leader %d", rf.me, entry, rf.Logs, args.LeaderId)
					}
					//else {
					//	reply.Term = rf.CurrentTerm
					//	reply.Success = false
					//	return
					//}
				}
				rf.mu.Unlock()
			}
			//args.PrevLogIndex = lastIndex(rf.Logs)
			//args.PrevLogTerm = lastTerm(rf.Logs)
			rf.mu.Lock()
		}
		//检查leader是否有更新的提交，如果有，那么在本地状态机应用并更新提交
		if args.LeaderCommit > rf.CommitIndex {
			//log.Printf("follower %d: commit from %d to %d", rf.me, rf.CommitIndex, args.LeaderCommit)
			commit := args.LeaderCommit
			if lastIndex(rf.Logs) < args.LeaderCommit {
				//log.Printf("follower %d: commit from %d to %d", rf.me, rf.CommitIndex, lastIndex(rf.Logs))
				commit = lastIndex(rf.Logs)
			}
			rf.CommitIndex = commit
			//log.Printf("commit %d", commit)
			for i := 0; i < rf.CommitIndex; i++ {
				rf.Logs[i].IsCommitted = true
			}
		}
		//args.PrevLogTerm = lastTerm(rf.Logs)
		//args.PrevLogIndex = lastIndex(rf.Logs)
	} else {
		if args.LeaderCommit > rf.CommitIndex {
			//log.Printf("follower %d: commit from %d to %d", rf.me, rf.CommitIndex, args.LeaderCommit)
			rf.CommitIndex = args.LeaderCommit
			commit := args.LeaderCommit
			if lastIndex(rf.Logs) < args.LeaderCommit {
				//log.Printf("follower %d: commit from %d to %d", rf.me, rf.CommitIndex, lastIndex(rf.Logs))
				commit = lastIndex(rf.Logs)
			}
			rf.CommitIndex = commit
			for i := 0; i < rf.CommitIndex; i++ {
				rf.Logs[i].IsCommitted = true
			}
		}
	}
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

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
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
		return
	}
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
	term = rf.CurrentTerm
	index = lastIndex(rf.Logs)
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.LogSignal <- true
		}
	}
	//log.Printf("leader %d insert %v to log, index is %d -> %v", rf.me, command, index, rf.Logs)
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

func (rf *Raft) updateCommit() {
	//match 超过半数与leader index一致就commit
	rf.mu.Lock()
	defer rf.mu.Unlock()
	count, alive := 0, 0
	//log.Printf("living server: %v", rf.CurrentServer)
	for _, isAlive := range rf.CurrentServer {
		if isAlive {
			alive += 1
		}
	}
	//log.Printf("alive number is %d", alive)
	for _, index := range rf.MatchIndex {
		if index == rf.MatchIndex[rf.me] {
			count += 1
			//log.Printf("count: %d", count)
		}
	}
	if count > alive/2 {
		log.Printf("leader %d commit log, index is %d", rf.me, rf.MatchIndex[rf.me])
		for i := rf.CommitIndex; i < rf.MatchIndex[rf.me]; i++ {
			rf.Logs[i].IsCommitted = true
		}
		rf.CommitIndex = rf.MatchIndex[rf.me]
	}
}

func (rf *Raft) doAppendEntries(peerId int) {
	go func(peer int) {
		rf.mu.Lock()
		var request AppendEntries
		select {
		case <-rf.LogSignal:
			request = AppendEntries{
				Term:          rf.CurrentTerm,
				LeaderId:      rf.me,
				LeaderCommit:  rf.CommitIndex,
				CurrentServer: rf.CurrentServer,
				PrevLogIndex:  rf.NextIndex[peer] - 1,
				PrevLogTerm:   lastTerm(rf.Logs),
				Entries:       rf.Logs,
			}
			break
		default:
			request = AppendEntries{
				Term:          rf.CurrentTerm,
				LeaderId:      rf.me,
				LeaderCommit:  rf.CommitIndex,
				CurrentServer: rf.CurrentServer,
			}
		}
		if len(request.Entries) != 0 {
			if request.PrevLogIndex == 0 {
				request.PrevLogTerm = lastTerm(rf.Logs)
				request.Entries = rf.Logs
			} else {
				request.PrevLogTerm = rf.Logs[request.PrevLogIndex-1].CreateTerm
				request.Entries = rf.Logs[request.PrevLogIndex:]
			}
		}
		log.Printf("next list: %v", rf.NextIndex)
		log.Printf("server %d (pre %d) send log address: %p", peer, request.PrevLogIndex, request.Entries)
		rf.mu.Unlock()
		reply := AppendEntriesReply{Alive: false, Success: false}
		ok := rf.sendAppendEntries(peer, &request, &reply)
		if ok {
			rf.mu.Lock()
			//检查是否中间选举过
			rf.MatchIndex[rf.me] = lastIndex(request.Entries)
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
			if reply.Alive {
				rf.CurrentServer[peer] = true
			} else {
				rf.CurrentServer[peer] = false
			}
			if len(request.Entries) != 0 {
				if reply.Success {
					//日志同步成功，leader需要更新nextIndex数组和matchIndex数组
					//nextIndex使用args里面的prevLogIndex更新（prevLogIndex直接在args中更新？）
					rf.NextIndex[peer] = request.PrevLogIndex + len(request.Entries) + 1
					//matchIndex使用nextIndex更新，他就是与leader匹配的那个index
					rf.MatchIndex[peer] = rf.NextIndex[peer] - 1
					log.Printf("leader %d next index list: %v", rf.me, rf.NextIndex)
					//log.Printf("leader %d match index list: %v", rf.me, rf.MatchIndex)
					rf.mu.Unlock()
					rf.updateCommit()
					rf.mu.Lock()
				} else {
					//todo 日志同步失败 回退next index bug：新leader初始化next index时不能更新选举前断掉的follower
					rf.NextIndex[peer] -= 1
					rf.MatchIndex[peer] = 0
					log.Printf("follower %d fail", peer)
				}
			}
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
				rf.LastActiveTime = now
				if rf.LeaderId != -1 {
					rf.CurrentServer[rf.LeaderId] = false
				}
				//构造投票请求，并发拉票
				requestVote := RequestVote{
					Term:         rf.CurrentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastIndex(rf.Logs),
					LastLogTerm:  lastIndex(rf.Logs),
				}
				maxTerm := rf.CurrentTerm
				me := rf.me
				serverNum := 0
				for i := 0; i < len(rf.peers); i++ {
					if rf.CurrentServer[i] {
						serverNum++
					}
				}
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
					return
				}
				//获取大多数选票，转换leader并return
				if voteNum > serverNum/2 {
					log.Printf("election: server %d be elected leader...", rf.me)
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
					return
				}
			}
		}()
	}
}

func (rf *Raft) applyLog() {
	//noMore := false
	for rf.killed() == false {
		//if noMore {
		//	time.Sleep(time.Millisecond * 10)
		//}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//判断commit和apply index关系，如果commit index变大，则需要有应用的条目
			for rf.CommitIndex > rf.LastApplied {
				rf.LastApplied += 1
				l := rf.Logs[rf.LastApplied-1]
				//如果需要提交，拼接ApplyMsg发送到applyLogSignal
				applyMsg := ApplyMsg{
					Command:      l.Command,
					CommandIndex: l.LogIndex,
					CommandValid: true,
				}
				rf.ApplyLogSignal <- applyMsg
				rf.Logs[rf.LastApplied-1].IsApplied = true
				//log.Printf("follower %d log %v", rf.me, rf.Logs)
				//如果需要继续提交，将more置为true
				//noMore = false
			}
			//noMore = true
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
	rf.CurrentServer = make(map[int]bool, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.CurrentServer[i] = true
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.election()
	go rf.appendLogSignal()
	go rf.applyLog()
	return rf
}
