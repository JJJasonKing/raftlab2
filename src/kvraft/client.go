package kvraft

import (
	"../labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	mu sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64	// 客户端唯一标识
	seqId int64	// 该客户端单调递增的请求id
	leaderId int
}

func (ck *Clerk) getClientID() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leaderId
}

func (ck *Clerk) addClientID() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	return ck.leaderId
}

// 产生cid
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	cmd := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	ldId := ck.getClientID()
	for  {
		reply := GetReply{}
		if ck.servers[ldId].Call("KVServer.Get", &cmd, &reply) {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			}
		}
		ldId = ck.addClientID()
		time.Sleep(1 * time.Millisecond)
	}
	// You will have to modify this function.
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// https://github.com/owenliang/mit-6.824
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	cmd := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	ldId := ck.getClientID()
	for  {
		reply := PutAppendReply{}
		if ck.servers[ldId].Call("KVServer.PutAppend", &cmd, &reply) {
			if reply.Err == OK {
				break
			}
		}
		ldId = ck.addClientID()
		time.Sleep(1 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
