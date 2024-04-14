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

// StreamMerger manages merging of sorted key-value pairs from multiple files.
type StreamMerger struct {
	readers []*bufio.Scanner
	pq      PriorityQueue
}

// NewStreamMerger initializes a StreamMerger with the given list of file names.
func NewStreamMerger(files []string) *StreamMerger {
	sm := &StreamMerger{
		readers: make([]*bufio.Scanner, len(files)),
		pq:      make(PriorityQueue, 0, len(files)),
	}
	heap.Init(&sm.pq)
	for i, file := range files {
		f, err := os.Open(file)
		if err != nil {
			fmt.Printf("Error opening file %s: %v\n", file, err)
			continue
		}
		sm.readers[i] = bufio.NewScanner(f)
		if sm.readers[i].Scan() {
			parts := strings.SplitN(sm.readers[i].Text(), ",", 2)
			heap.Push(&sm.pq, &Item{key: parts[0], value: parts[1], index: i})
		}
	}
	return sm
}

// Merge outputs the merged stream of key-value pairs to the CLI.
func (sm *StreamMerger) Merge() {
	var currentKey string
	var buffer []string

	for sm.pq.Len() > 0 {
		minItem := heap.Pop(&sm.pq).(*Item)

		// Check if we need to output the current buffer
		if minItem.key != currentKey && currentKey != "" {
			// TODO: instead pass it to user's reducer
			fmt.Printf("%s: %v\n", currentKey, buffer)
			buffer = buffer[:0]
		}

		// Update the current key and append the new value to the buffer
		currentKey = minItem.key
		buffer = append(buffer, minItem.value)

		// Get the next item from the same file
		if sm.readers[minItem.index].Scan() {
			parts := strings.SplitN(sm.readers[minItem.index].Text(), ",", 2)
			if len(parts) == 2 {
				heap.Push(&sm.pq, &Item{key: parts[0], value: parts[1], index: minItem.index})
			}
		}
	}

	// Output any remaining items in the buffer
	if len(buffer) > 0 {
		fmt.Printf("%s: %v\n", currentKey, buffer)
	}
}
