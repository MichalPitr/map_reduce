package mapper

import (
	"bufio"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/MichalPitr/map_reduce/pkg/config"
	"github.com/MichalPitr/map_reduce/pkg/interfaces"
)

var fnvHash hash.Hash32 = fnv.New32a()

// TextInput implements the MapInput interface for simple strings.
type TextInput struct {
	data string
}

func (ti *TextInput) Value() string {
	return ti.data
}

var registeredMappers = make(map[string]func() interfaces.Mapper)

func RegisterMapper(cfg *config.Config, name string, factory func() interfaces.Mapper) {
	registeredMappers[name] = factory
	cfg.MapperClass = name
}

func GetMapper(name string) interfaces.Mapper {
	if factory, exists := registeredMappers[name]; exists {
		return factory()
	}
	log.Fatalf("Mapper not registered: %s", name)
	return nil
}

func Run(cfg *config.Config) {
	log.Printf("Running mapper...")
	mapper := GetMapper(cfg.MapperClass)
	processFiles(cfg, mapper)
}

func processFiles(cfg *config.Config, mapper interfaces.Mapper) {
	prefix, start, end := parseFileRange(cfg.FileRange)
	intermediate := make(map[string][]string)

	// Prepare output dir
	if err := os.MkdirAll(cfg.OutputDir, 0777); err != nil {
		log.Printf("Creating directory %s failed: %v", cfg.OutputDir, err)
		os.Exit(1)
	}

	emit := func(key, value string) {
		intermediate[key] = append(intermediate[key], value)
	}

	for i := start; i <= end; i++ {
		filePath := fmt.Sprintf("%s/%s-%d", cfg.InputDir, prefix, i)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("Failed to open file %s: %v", filePath, err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			// TODO: generalize for other input types maybe?
			input := &TextInput{data: scanner.Text()}
			mapper.Map(input, emit)
		}

		if err := scanner.Err(); err != nil {
			log.Fatalf("error reading from file %s: %v", filePath, err)
		}
	}

	flushData(cfg.OutputDir, cfg.NumReducers, intermediate)
}

func parseFileRange(fileRange string) (string, int, int) {
	substrings := strings.Split(fileRange, "-")
	if len(substrings) != 3 {
		log.Printf("Expected file range in format prefix-start-end but got %s.", fileRange)
		os.Exit(1)
	}
	prefix := substrings[0]
	start, err := strconv.Atoi(substrings[1])
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	end, err := strconv.Atoi(substrings[2])
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	return prefix, start, end
}

func flushData(outputDir string, numPartitions int, intermediate map[string][]string) {
	for key, values := range intermediate {
		partition := getKeyPartition(key, numPartitions)
		fileName := fmt.Sprintf("%s/partition-%d", outputDir, partition)
		writeToFile(fileName, key, values)
	}
}

func writeToFile(fileName, key string, values []string) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open file %s: %v", fileName, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()
	for _, value := range values {
		_, err = writer.WriteString(fmt.Sprintf("%s,%s\n", key, value))
		if err != nil {
			log.Fatalf("Failed to write to file %s: %v", fileName, err)
		}
	}
}

func getKeyPartition(key string, numPartitions int) int {
	hash, err := fnvHash.Write([]byte(key))
	if err != nil {
		log.Fatalf("Error calculating hash: %v", err)
	}
	defer fnvHash.Reset()
	return hash % numPartitions
}
