package reducer

import (
	"fmt"
	"log"
	"os"

	"github.com/MichalPitr/map_reduce/pkg/config"
	"github.com/MichalPitr/map_reduce/pkg/interfaces"
)

var registeredReducers = make(map[string]func() interfaces.Reducer)

func RegisterReducer(cfg *config.Config, name string, factory func() interfaces.Reducer) {
	registeredReducers[name] = factory
	cfg.ReducerClass = name
}

func GetReducer(name string) interfaces.Reducer {
	if factory, exists := registeredReducers[name]; exists {
		return factory()
	}

	log.Fatalf("Reducer not registered: %s", name)
	return nil
}

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

	// Start reading partitions and on-the-fly merge.
	sm := NewStreamMerger(partitionFiles)

	// TODO: Pass key-value pairs to user's reducer.
	sm.Merge()
}
