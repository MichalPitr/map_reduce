package mapper

import (
	"bufio"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/MichalPitr/map_reduce/pkg/config"
)

var fnvHash hash.Hash32 = fnv.New32a()

// TextInput implements the MapInput interface for simple strings.
type TextInput struct {
	data string
}

func (ti *TextInput) Value() string {
	return ti.data
}

func Run(cfg *config.Config) {
	log.Printf("Running mapper...")
	processFiles(cfg)
}

func processFiles(cfg *config.Config) {
	mapper := cfg.Mapper
	prefix, start, end := parseFileRange(cfg.FileRange)

	// Prepare output dir
	mustCreateOutputDir(cfg.OutputDir)

	intermediate := make(map[string][]string)
	emit := func(key, value string) {
		intermediate[key] = append(intermediate[key], value)
	}

	for i := start; i <= end; i++ {
		fName := fmt.Sprintf("%s-%d", prefix, i)
		filePath := filepath.Join(cfg.InputDir, fName)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("Failed to open file %s: %v", filePath, err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			// TODO: generalize for other input types maybe.
			input := &TextInput{data: scanner.Text()}
			mapper.Map(input, emit)
		}

		if err := scanner.Err(); err != nil {
			log.Fatalf("error reading from file %s: %v", filePath, err)
		}
	}

	flushData(cfg.OutputDir, cfg.NumReducers, intermediate)
}

func mustCreateOutputDir(dir string) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		log.Fatalf("Creating directory %s failed: %v", dir, err)
	}
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
	// Sort keys to write in alphabetic order.
	keys := make([]string, 0, len(intermediate))
	for key := range intermediate {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Prepare output files
	writers := make([]*bufio.Writer, 0, numPartitions)
	for p := range numPartitions {
		partitionName := fmt.Sprintf("partition-%d", p)
		fileName := filepath.Join(outputDir, partitionName)
		file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Failed to open file %s: %v", fileName, err)
		}
		defer file.Close()
		writer := bufio.NewWriter(file)
		defer writer.Flush()
		writers = append(writers, writer)
	}

	// Write to files
	for _, key := range keys {
		p := getKeyPartition(key, numPartitions)
		writeToFile(writers[p], key, intermediate[key])
	}
}

func writeToFile(writer *bufio.Writer, key string, values []string) {
	for _, value := range values {
		_, err := writer.WriteString(fmt.Sprintf("%s,%s\n", key, value))
		if err != nil {
			log.Fatalf("Failed to write to file: %v", err)
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
