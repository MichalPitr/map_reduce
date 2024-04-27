package reducer

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/MichalPitr/map_reduce/pkg/config"
)

func Run(cfg *config.Config) {
	log.Printf("Running reducer...")
	log.Printf("Reducer input dir: %s", cfg.InputDir)
	partitionFiles := make([]string, 0, cfg.NumReducers)
	inputFiles, err := os.ReadDir(cfg.InputDir)
	if err != nil {
		log.Fatalf("Failed to read dir %s: %v", cfg.InputDir, err)
	}

	for _, file := range inputFiles {
		if !file.IsDir() {
			continue
		}
		partitionName := fmt.Sprintf("partition-%d", cfg.ReducerId)

		partition := filepath.Join(cfg.InputDir, file.Name(), partitionName)
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

	// Prepare output dir
	if err := os.MkdirAll(cfg.OutputDir, 0777); err != nil {
		log.Fatalf("Creating directory %s failed: %v", cfg.OutputDir, err)
	}

	//Save results to disk, probably to job-id/out/reducer-{id} file.

	outputFilePath := filepath.Join(cfg.OutputDir, fmt.Sprintf("reducer-%d", cfg.ReducerId))
	file, err := os.OpenFile(outputFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	for key, values := range results {
		_, err := writer.WriteString(fmt.Sprintf("%s,%s\n", key, values[0]))
		if err != nil {
			log.Fatalf("Failed to write to a file: %v", err)
		}
	}
}
