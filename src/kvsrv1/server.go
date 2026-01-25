package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	//存储 key -> (value, version)
	data map[string]struct {
		value string
		version rpc.Tversion
	}
}

// 初始化 Server
func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.data = make(map[string]struct {
		value string
		version rpc.Tversion
	})
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	// 加锁确保线程安全
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查 key 是否存在
	entry, exists := kv.data[args.Key]
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}

	// key 存在，返回对应的值和版本号
	reply.Value = entry.value
	reply.Version = entry.version
	reply.Err = rpc.OK	// 表示操作成功
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, exists := kv.data[args.Key]

	if !exists {
		if args.Version == 0 {
			//创建新 key, version 从0 -> 1
			kv.data[args.Key] = struct {
				value string
				version rpc.Tversion
			}{value: args.Value, version: 1}
			reply.Err = rpc.OK
		}else {
			// version > 0 但 key不存在，返回 ErrNoKey
			reply.Err = rpc.ErrNoKey
		}
	}else {
		if args.Version == entry.version {
			//版本匹配,可以安全更新
			entry.value = args.Value		// 更新值
			entry.version++					// 版本号递增
			kv.data[args.Key] = entry		// 写回 map
			reply.Err = rpc.OK				// 更新成功
		}else {
			//版本不匹配
			reply.Err = rpc.ErrVersion
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
