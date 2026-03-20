package tcore

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

/*
chunkList (分区列表)
	│
	├── head ──→ [Mutable chunk 1] ←── 最新分区（可写）
	│              │ next
	│              ↓
	│           [Mutable chunk 2] ←── 次新分区（可写，用于乱序）
	│              │ next
	│              ↓
	│           [Immutable chunk 1]   ←── 只读
	│              │ next
	│              ↓
	│           [Immutable chunk 2]   ←── 只读
	│              │ next
	│              ↓
	│           [Immutable chunk 3]   ←── 最旧分区
	│              │ next = nil
	│
	└── tail ──→ [Immutable chunk 3]   ←── 尾指针
*/

type chunkList struct {
	numChunks int64
	head      *chunkNode
	tail      *chunkNode
	mu        sync.RWMutex
}

func newChunkList() *chunkList {
	return &chunkList{}
}

func (l *chunkList) getHead() chunk {
	if l.count() <= 0 {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.head.chunk()
}

func (l *chunkList) insert(chunk chunk) {
	node := &chunkNode{
		c: chunk,
	}
	l.mu.RLock()
	head := l.head
	l.mu.RUnlock()
	if head != nil {
		node.next = head
	}

	l.setHead(node)
	atomic.AddInt64(&l.numChunks, 1)
}

func (l *chunkList) remove(target chunk) error {
	if l.count() <= 0 {
		return fmt.Errorf("empty chunk")
	}

	// 从头开始遍历自身。
	var prev, next *chunkNode
	iterator := l.newIterator()
	for iterator.next() {
		current := iterator.currentNode()
		if !samechunk(current.chunk(), target) {
			prev = current
			continue
		}

		// 删除当前节点。

		iterator.next()
		next = iterator.currentNode()
		switch {
		case prev == nil:
			// 删除头节点
			l.setHead(next)
		case next == nil:
			// 删除尾节点
			prev.setNext(nil)
			l.setTail(prev)
		default:
			// 删除中间节点
			prev.setNext(next)
		}
		atomic.AddInt64(&l.numChunks, -1)

		if err := current.chunk().clean(); err != nil {
			return fmt.Errorf("failed to clean resources managed by chunk to be removed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("the given chunk was not found")
}

func (l *chunkList) swap(old, new chunk) error {
	if l.count() <= 0 {
		return fmt.Errorf("empty chunk")
	}

	// 从头开始遍历自身。
	var prev, next *chunkNode
	iterator := l.newIterator()
	for iterator.next() {
		current := iterator.currentNode()
		if !samechunk(current.chunk(), old) {
			prev = current
			continue
		}

		// 交换当前节点。

		newNode := &chunkNode{
			c:    new,
			next: current.getNext(),
		}
		iterator.next()
		next = iterator.currentNode()
		switch {
		case prev == nil:
			// 交换头节点
			l.setHead(newNode)
		case next == nil:
			// 交换尾节点
			prev.setNext(newNode)
			l.setTail(newNode)
		default:
			prev.setNext(newNode)
		}
		return nil
	}

	return fmt.Errorf("the given chunk was not found")
}

func samechunk(x, y chunk) bool {
	return x.minTimestamp() == y.minTimestamp()
}

func (l *chunkList) count() int {
	return int(atomic.LoadInt64(&l.numChunks))
}

func (l *chunkList) newIterator() *chunkIterator {
	l.mu.RLock()
	head := l.head
	l.mu.RUnlock()
	// 使用 dummy，这样第一次调用 next() 能定位到头节点
	dummy := &chunkNode{
		next: head,
	}
	return &chunkIterator{
		current: dummy,
	}
}

func (l *chunkList) setHead(node *chunkNode) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.head = node
}

func (l *chunkList) setTail(node *chunkNode) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tail = node
}

func (l *chunkList) String() string {
	b := &strings.Builder{}
	iterator := l.newIterator()
	for iterator.next() {
		chunk := iterator.chunk()
		if _, ok := chunk.(*mutableChunk); ok {
			b.WriteString("[Mutable chunk]")
		} else if _, ok := chunk.(*immutableChunk); ok {
			b.WriteString("[Immutable chunk]")
		} else {
			b.WriteString("[Unknown chunk]")
		}
		b.WriteString("->")
	}
	return strings.TrimSuffix(b.String(), "->")
}

type chunkNode struct {
	c    chunk
	next *chunkNode
	mu   sync.RWMutex
}

func (node *chunkNode) chunk() chunk {
	return node.c
}

func (node *chunkNode) setNext(nextNode *chunkNode) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.next = nextNode
}

func (node *chunkNode) getNext() *chunkNode {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.next
}

// chunkIterator 表示分区列表的迭代器。基本用法如下：
/*
  for iterator.next() {
    chunk, err := iterator.chunkue()
    // 使用分区做些什么
  }
*/
type chunkIterator struct {
	current *chunkNode
}

func (i *chunkIterator) next() bool {
	if i.current == nil {
		return false
	}
	next := i.current.getNext()
	i.current = next
	return i.current != nil
}

func (i *chunkIterator) chunk() chunk {
	if i.current == nil {
		return nil
	}
	return i.current.chunk()
}

func (i *chunkIterator) currentNode() *chunkNode {
	return i.current
}
