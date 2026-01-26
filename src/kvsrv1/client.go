package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)


type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
// 可靠网络下的 Get 方法
// func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
// 	// You will have to modify this function.
// 	// 准备 RPC 调用参数
// 	args := rpc.GetArgs{Key: key}
// 	reply := rpc.GetReply{}

// 	// 发起 RPC 调用到服务器的 Get 方法
// 	ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)

// 	if !ok {
// 		// 可靠网络下不应该发生
// 		return "", 0, rpc.ErrNoKey
// 	}

// 	return reply.Value, reply.Version, reply.Err
// }

// 不可靠网络下的 Get 方法
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	// 准备 RPC 调用参数
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok {
			// 收到回复， 返回结果
			return reply.Value, reply.Version, reply.Err
		} else {
			//没有收到回复，等待后重试
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
// 可靠网络下的 Put 方法
// func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
// 	// You will have to modify this function.
// 	//准备 RPC 调用参数
// 	args := rpc.PutArgs {
// 		Key: key,
// 		Value: value,
// 		Version: version,
// 	}
// 	reply := rpc.PutReply{}

// 	//发起 RPC 调用到服务器的 Put 方法
// 	ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)

// 	if !ok {
// 		return rpc.ErrNoKey
// 	}

// 	return reply.Err
// }

// 不可靠网络下的 Put 方法（添加重试和 ErrMaybe 处理）
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	//准备 RPC 调用参数
	args := rpc.PutArgs {
		Key: key,
		Value: value,
		Version: version,
	}
	reply := rpc.PutReply{}

	isFirstAttempt := true

	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if ok {
			// 收到回复
			if reply.Err == rpc.ErrVersion {
				//收到 ErrVersion
				if isFirstAttempt {
					// 第一次请求就收到 ErrAttempt，说明 Put 一定没有执行
					return rpc.ErrVersion
				} else {
					//重试后收到 ErrVersion，可能是第一次执行了但回复丢失
					// 返回 ErrMaybe
					return rpc.ErrMaybe
				}
			}
			// 其他错误或成功，直接返回
			return reply.Err
		} else {
			// 没有收到回复，标记为不是第一次尝试
			isFirstAttempt = false
			// 等待后重试
			time.Sleep(100 * time.Millisecond)
		}
	}
}
