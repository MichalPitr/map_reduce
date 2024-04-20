package reducer

import (
	"fmt"
	"log"
	"os"

	"github.com/MichalPitr/map_reduce/pkg/config"
)

func Run(cfg *config.Config) {
	log.Printf("Running reducer...")

	// Get slice of input files
	basePath := fmt.Sprintf("%s/%s/", cfg.NfsPath, cfg.JobId)
	log.Printf("base path: %s", basePath)
	partitionFiles := make([]string, 0, cfg.NumReducers)
	inputFiles, err := os.ReadDir(basePath)
	if err != nil {
		log.Fatalf("Failed to read dir %s: %v", basePath, err)
	}

	for _, file := range inputFiles {
		if !file.IsDir() {
			continue
		}
		partition := fmt.Sprintf("%s/%s/partition-%d", basePath, file.Name(), cfg.ReducerId)
		partitionFiles = append(partitionFiles, partition)
	}

	results := make(map[string][]string)
	reducer := cfg.Reducer

	// Start reading partitions and on-the-fly merge.
	sm := NewStreamMerger(partitionFiles)
	for sm.pq.Len() > 0 {
		key := sm.Key()
		emit := func(value string) {
			results[key] = append(results[key], value)
		}

		reducer.Reduce(sm, emit)
		// reset so that we can process the next key
		sm.done = false
	}

	//TODO: Save results to disk, probably to job-id/reducer-{id} file.
}
