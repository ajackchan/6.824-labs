package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk	// 连接到 KV 服务器的客户端
	// You may add code here
	lockKey string	// 存储锁状态的 key
	clientID string	// 每个锁客户端的唯一标识
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck: ck,
		lockKey: l,
		clientID: kvtest.RandValue(8),	// 生成 8 字节随机字符串作为客户端 ID
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {	// 无限循环，直到成功获取锁
		// 1.获取当前锁状态
		value, version, err := lk.ck.Get(lk.lockKey)

		if err == rpc.ErrNoKey {
			// key 不存在。尝试获取锁（创建 key，version=0）
			err := lk.ck.Put(lk.lockKey, lk.clientID, 0)
			if err == rpc.OK {
				// 成功获取锁
				return 
			}
			// 可能是并发竞争，继续重试
		} else if err == rpc.OK {
			// key 存在
			if value == "" {
				// 锁空闲（空字符串表示未加锁）,尝试获取
				err := lk.ck.Put(lk.lockKey, lk.clientID, version)
				if err == rpc.OK {
					// 成功获取锁
					return 
				}
				// 版本不匹配，可能是其他客户端获取了锁，继续重试
			}
			// 锁被其他客户端持有，等待
		}

		// 等待一段时间后重试
		time.Sleep(10 * time.Millisecond)
 	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		// 获取当前锁状态
		value, version, err := lk.ck.Get(lk.lockKey)

		if err == rpc.ErrNoKey {
			// key 不存在，说明锁已经被释放
			return 
		}

		if err == rpc.OK {
			if value == lk.clientID {
				// 确认锁由当前客户端持有，释放锁（设置为空字符串）
				err := lk.ck.Put(lk.lockKey, "", version)
				if err == rpc.OK {
					// 成功释放
					return 
				}
				// 版本不匹配，可能锁状态已改变，继续重试
			} else {
				// 锁不是由当前客户端持有，不应该释放
				return 
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}
