package mapper

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/MichalPitr/map_reduce/pkg/config"
	"github.com/MichalPitr/map_reduce/pkg/interfaces"
)

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

	writeToCSV(cfg.OutputDir, "output.csv", intermediate)
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

// writeToCSV takes a map of keys to slices of values and writes them to a CSV file.
func writeToCSV(outputDir, filename string, data map[string][]string) {
	// Create the output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Open a file for writing
	filePath := outputDir + "/" + filename
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Iterate over the data and write to the CSV file
	for key, values := range data {
		for _, value := range values {
			record := []string{key, value}
			if err := writer.Write(record); err != nil {
				log.Fatalf("Failed to write record to CSV: %v", err)
			}
		}
	}

	log.Printf("Data successfully written to %s", filePath)
}
