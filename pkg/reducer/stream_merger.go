package reducer

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"strings"
)

// Item represents a key-value pair along with the index of the source file.
type Item struct {
	key   string
	value string
	index int
}

// PriorityQueue implements heap.Interface to manage a min-heap based on key values.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int           { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool { return pq[i].key < pq[j].key }
func (pq PriorityQueue) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i] }

func (pq *PriorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*Item))
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func (pq PriorityQueue) Peek() *Item {
	if len(pq) > 0 {
		return pq[0]
	}
	return nil
}

// StreamMerger manages merging of sorted key-value pairs from multiple files.
type StreamMerger struct {
	readers []*bufio.Scanner
	pq      PriorityQueue
	done    bool
}

func NewStreamMerger(files []string) *StreamMerger {
	sm := &StreamMerger{
		readers: make([]*bufio.Scanner, len(files)),
		pq:      make(PriorityQueue, 0),
	}
	heap.Init(&sm.pq)
	for i, file := range files {
		f, err := os.Open(file)
		if err != nil {
			fmt.Printf("Error opening file %s: %v\n", file, err)
			continue
		}
		reader := bufio.NewScanner(f)
		sm.readers[i] = reader
		if reader.Scan() {
			parts := strings.SplitN(reader.Text(), ",", 2)
			heap.Push(&sm.pq, &Item{key: parts[0], value: parts[1], index: i})
		}
	}
	return sm
}

func (sm *StreamMerger) Key() string {
	item := sm.pq.Peek()
	if item == nil {
		return ""
	}
	return item.key
}

func (sm *StreamMerger) Value() string {
	item := sm.pq.Peek()
	if item == nil {
		return ""
	}
	return item.value
}

func (sm *StreamMerger) NextValue() {
	if sm.pq.Len() == 0 {
		return
	}
	item := heap.Pop(&sm.pq).(*Item)

	// Read new key-value pair from the file that was just popped
	if sm.readers[item.index].Scan() {
		parts := strings.SplitN(sm.readers[item.index].Text(), ",", 2)
		if len(parts) == 2 {
			heap.Push(&sm.pq, &Item{key: parts[0], value: parts[1], index: item.index})
		}
	}

	if sm.pq.Len() > 0 && sm.pq.Peek().key != item.key {
		sm.done = true
	}
}

func (sm *StreamMerger) Done() bool {
	return sm.done || sm.pq.Len() == 0
}
