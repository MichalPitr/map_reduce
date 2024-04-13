package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/MichalPitr/map_reduce/pkg/config"
	"github.com/MichalPitr/map_reduce/pkg/master"
)

func main() {
	cfg := config.ParseFlags()

	switch cfg.Mode {
	case "master":
		master.RunMaster(cfg)
	case "mapper":
		runMapper(cfg)
	case "reducer":
		runReducer(cfg)
	default:
		log.Printf("Invalid mode specified: %q", cfg.Mode)
		os.Exit(128)
	}
}

func runMapper(cfg *config.Config) {
	log.Printf("Running mapper...")

	substrings := strings.Split(cfg.FileRange, "-")
	if len(substrings) != 3 {
		log.Printf("Expected file range in format prefix-start-end but got %s.", cfg.FileRange)
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

	// Prepare output dir
	if err := os.MkdirAll(cfg.OutputDir, 0777); err != nil {
		log.Printf("Creating directory %s failed: %v", cfg.OutputDir, err)
		os.Exit(1)
	}

	// Do Mapper operation. This should be the pluggable part defined by user.
	wordFreq := make(map[string]int)
	re := regexp.MustCompile(`\b\w+\b`)
	for i := start; i <= end; i++ {
		fileName := fmt.Sprintf("%s-%d", prefix, i)

		file, err := os.Open(cfg.InputDir + fileName)
		if err != nil {
			fmt.Println("Error opening file:", err)
			os.Exit(1)
		}
		defer file.Close()

		// Create a scanner to read the file
		scanner := bufio.NewScanner(file)

		// Iterate over each line in the file
		for scanner.Scan() {
			// Split the line into words
			line := scanner.Text()
			line = strings.ToLower(line)
			words := re.FindAllString(line, -1)
			// words := strings.Fields(line)

			// Iterate over each word
			for _, word := range words {
				wordFreq[word]++
			}
		}

		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading from file:", err)
		}
	}

	// Save result to NFS storage
	file, err := os.Create(cfg.OutputDir + "output.csv")
	if err != nil {
		log.Printf("Failed to create a file: %v", err)
		os.Exit(1)
	}
	defer file.Close()

	// Create a buffered writer
	writer := bufio.NewWriter(file)

	// Iterate over the map and write to file
	for key, value := range wordFreq {
		line := fmt.Sprintf("%s,%d\n", key, value)
		if _, err := writer.WriteString(line); err != nil {
			log.Printf("Failed to create a file: %v", err)
			os.Exit(1)
		}
	}

	// Flush any buffered data
	if err := writer.Flush(); err != nil {
		log.Printf("Failed to create a file: %v", err)
		os.Exit(1)
	}
}

func runReducer(cfg *config.Config) {
	log.Printf("Running reducer...")
}
