package reducer

import "testing"

func TestMerge(t *testing.T) {
	files := []string{"/mnt/test_files/file-1", "/mnt/test_files/file-2"}
	sm := NewStreamMerger(files)
	sm.Merge()
}
