package reducer

import (
	"fmt"
	"testing"
)

func TestMerge(t *testing.T) {
	files := []string{"/mnt/test_files/file-1", "/mnt/test_files/file-2"}
	sm := NewStreamMerger(files)
	for sm.pq.Len() > 0 {
		fmt.Printf("Key: %s\n", sm.Key())
		for !sm.Done() {
			fmt.Println(sm.Value())
			sm.NextValue()
		}
		// reset so that we can process the next key
		sm.done = false
	}
}

func TestMergeEmptyFiles(t *testing.T) {
	files := []string{"/mnt/test_files/file-3"}
	sm := NewStreamMerger(files)
	for sm.pq.Len() > 0 {
		fmt.Printf("Key: %s\n", sm.Key())
		for !sm.Done() {
			fmt.Println(sm.Value())
			sm.NextValue()
		}
		// reset so that we can process the next key
		sm.done = false
	}
}
