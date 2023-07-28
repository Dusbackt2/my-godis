package dict

import (
	"sync"
	"sync/atomic"
)

type Dict struct {
	table       atomic.Value // []*Shard
	nextTable   []*Shard
	nextTableMu sync.Mutex
	count       int32

	rehashIndex int32
}

type Node struct {
	key      string
	val      interface{}
	next     *Node
	hashCode uint32
}

type Shard struct {
	head  *Node
	mutex sync.RWMutex
}

const (
	maxCapacity      = 1 << 15
	minCapacity      = 16
	rehashConcurrent = 4
	loadFactor       = 0.75
)

// return the mini power of two which is not less than cap
// See Hackers Delight, sec 3.2
func computeCapacity(param int) (size int) {
	if param <= minCapacity {
		return minCapacity
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 || n >= maxCapacity {
		return maxCapacity
	} else {
		return int(n + 1)
	}
}

func Make(shardCountHint int) *Dict {
	shardCount := computeCapacity(shardCountHint)
	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{}
	}
	d := &Dict{
		count:       0,
		rehashIndex: -1,
	}
	d.table.Store(table)
	return d
}

const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (dict *Dict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	table, _ := dict.table.Load().([]*Shard)
	tableSize := uint32(len(table))
	return (tableSize - 1) & uint32(hashCode)
}

func (dict *Dict) getShard(index uint32) *Shard {
	if dict == nil {
		panic("dict is nil")
	}
	table, ok := dict.table.Load().([]*Shard)
	if !ok {
		panic("load table failed")
	}
	return table[index]
}

func (dict *Dict) getNextShard(hashCode uint32) *Shard {
	if dict == nil {
		panic("dict is nil")
	}
	if dict.nextTable == nil {
		panic("next table is nil")
	}
	nextTableSize := uint32(len(dict.nextTable))
	index := (nextTableSize - 1) & uint32(hashCode)
	return dict.nextTable[index]
}

func (dict *Dict) ensureNextTable() {
	if dict.nextTable == nil {
		dict.nextTableMu.Lock()

		// check-lock-check
		if dict.nextTable == nil {
			table, _ := dict.table.Load().([]*Shard)
			tableSize := uint32(len(table))
			// init next table
			nextShardCount := tableSize * 2
			if nextShardCount > maxCapacity || nextShardCount < 0 {
				nextShardCount = maxCapacity
			}
			if nextShardCount <= tableSize {
				// reach limit, cannot resize
				atomic.StoreInt32(&dict.rehashIndex, -1)
				return
			}
			nextTable := make([]*Shard, nextShardCount)
			var i uint32
			for i = 0; i < nextShardCount; i++ {
				nextTable[i] = &Shard{}
			}
			dict.nextTable = nextTable
		}

		dict.nextTableMu.Unlock()
	}
}

func (shard *Shard) Get(key string) (val interface{}, exists bool) {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	node := shard.head
	for node != nil {
		if node.key == key {
			return node.val, true
		}
		node = node.next
	}
	return nil, false
}

func (dict *Dict) Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	rehashIndex := atomic.LoadInt32(&dict.rehashIndex)
	if rehashIndex >= int32(index) {
		/* if rehashIndex > index. then the shard has finished resize, put in next table
		 * if rehashIndex == index, the shard may be resizing or just finished.
		 * Resizing will not be finished until the lock has been released
		 */
		dict.ensureNextTable()
		nextShard := dict.getNextShard(hashCode)
		val, exists = nextShard.Get(key)
	} else {
		/*
		 * if rehashing not in progress or the shard has not been rehashing, put in current shard
		 */
		shard := dict.getShard(index)
		val, exists = shard.Get(key)
	}

	return
}

func (dict *Dict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&dict.count))
}

func (shard *Shard) Put(key string, val interface{}, hashCode uint32) int {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	node := shard.head
	if node == nil {
		// empty shard
		node = &Node{
			key:      key,
			val:      val,
			hashCode: hashCode,
		}
		shard.head = node
		return 1
	} else {
		for {
			if node.key == key {
				// existed node
				node.val = val
				return 0
			}
			if node.next == nil {
				// append
				node.next = &Node{
					key:      key,
					val:      val,
					hashCode: hashCode,
				}
				return 1
			}
			node = node.next
		}
	}
}

// return the number of new inserted key-value
func (dict *Dict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	rehashIndex := atomic.LoadInt32(&dict.rehashIndex)
	if rehashIndex >= int32(index) {
		/* if rehashIndex > index. then the shard has finished resize, put in next table
		 * if rehashIndex == index, the shard may be resizing or just finished.
		 * Resizing will not be finished until the lock has been released
		 */
		dict.ensureNextTable()
		nextShard := dict.getNextShard(hashCode)
		result = nextShard.Put(key, val, hashCode)
	} else {
		/*
		 * if rehashing not in progress or the shard has not been rehashing, put in current shard
		 */
		shard := dict.getShard(index)
		result = shard.Put(key, val, hashCode)
	}
	if result == 1 {
		dict.addCount()
	}
	return result
}

func (shard *Shard) PutIfAbsent(key string, val interface{}, hashCode uint32) int {
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	node := shard.head
	if node == nil {
		// empty shard
		node = &Node{
			key:      key,
			val:      val,
			hashCode: hashCode,
		}
		shard.head = node
		return 1
	} else {
		for {
			if node.key == key {
				// existed node
				return 0
			}
			if node.next == nil {
				// append
				node.next = &Node{
					key:      key,
					val:      val,
					hashCode: hashCode,
				}
				return 1
			}
			node = node.next
		}
	}
}

// return the number of updated key-value
func (dict *Dict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)

	rehashIndex := atomic.LoadInt32(&dict.rehashIndex)
	if rehashIndex >= int32(index) {
		dict.ensureNextTable()
		nextShard := dict.getNextShard(hashCode)
		result = nextShard.PutIfAbsent(key, val, hashCode)
	} else {
		shard := dict.getShard(index)
		result = shard.PutIfAbsent(key, val, hashCode)
	}
	if result == 1 {
		dict.addCount()
	}
	return result
}

func (shard *Shard) PutIfExists(key string, val interface{}) int {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	node := shard.head
	for node != nil {
		if node.key == key {
			node.val = val
			return 1
		}
		node = node.next
	}
	return 0
}

// return the number of updated key-value
func (dict *Dict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)

	rehashIndex := atomic.LoadInt32(&dict.rehashIndex)
	if rehashIndex >= int32(index) {
		dict.ensureNextTable()
		nextShard := dict.getNextShard(hashCode)
		result = nextShard.PutIfExists(key, val)
	} else {
		shard := dict.getShard(index)
		result = shard.PutIfExists(key, val)
	}
	return
}

func (shard *Shard) Remove(key string) int {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	node := shard.head
	if node == nil {
		// empty shard
		return 0
	} else if node.key == key {
		// remove first node
		shard.head = node.next
		return 1
	} else {
		prev := node
		node = node.next
		for node != nil {
			if node.key == key {
				prev.next = node.next
				return 1
			}
			prev = node
			node = node.next
		}
	}
	return 0
}

// return the number of deleted key-value
func (dict *Dict) Remove(key string) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	rehashIndex := atomic.LoadInt32(&dict.rehashIndex)
	if rehashIndex >= int32(index) {
		dict.ensureNextTable()
		nextShard := dict.getNextShard(hashCode)
		result = nextShard.Remove(key)
	} else {
		shard := dict.getShard(dict.spread(hashCode))
		result = shard.Remove(key)
	}
	if result > 0 {
		atomic.AddInt32(&dict.count, -1)
	}
	return
}

func (dict *Dict) addCount() int32 {
	count := atomic.AddInt32(&dict.count, 1)
	table, _ := dict.table.Load().([]*Shard)
	if float64(count) >= float64(len(table))*loadFactor {
		dict.resize()
	}
	return count
}

func (dict *Dict) resize() {
	if !atomic.CompareAndSwapInt32(&dict.rehashIndex, -1, 0) {
		// resize already in progress
		return
	}
	dict.ensureNextTable()

	var wg sync.WaitGroup
	wg.Add(rehashConcurrent)
	for i := 0; i < rehashConcurrent; i++ {
		go dict.transfer(&wg)
	}
	wg.Wait()

	// finish rehash
	dict.table.Store(dict.nextTable)
	dict.nextTable = nil
	atomic.StoreInt32(&dict.rehashIndex, -1)
}

func (dict *Dict) transfer(wg *sync.WaitGroup) {
	table, _ := dict.table.Load().([]*Shard)
	tableSize := uint32(len(table))
	// dict.rehashIndex must >= 0
	for {
		i := uint32(atomic.AddInt32(&dict.rehashIndex, 1)) - 1
		if i >= tableSize {
			wg.Done()
			return
		}
		shard := dict.getShard(i)
		shard.mutex.RLock()

		nextShard0 := dict.nextTable[i]
		nextShard1 := dict.nextTable[i+tableSize]

		nextShard0.mutex.RLock()
		nextShard1.mutex.RLock()

		var head0, head1 *Node
		var tail0, tail1 *Node
		node := shard.head
		for node != nil {
			// split current shard to 2 shards in next table
			if node.hashCode&tableSize == 0 {
				if head0 == nil {
					head0 = node
				} else {
					tail0.next = node
				}
				tail0 = node
			} else {
				if head1 == nil {
					head1 = node
				} else {
					tail1.next = node
				}
				tail1 = node
			}
			node = node.next
		}

		if tail0 != nil {
			tail0.next = nil

			nextShard0.head = head0
		}

		if tail1 != nil {
			tail1.next = nil

			nextShard1.head = head1
		}

		nextShard1.mutex.RUnlock()
		nextShard0.mutex.RUnlock()
		shard.mutex.RUnlock()
	}
}
