package mapper

import (
	"regexp"
	"strings"
	"testing"

	"github.com/MichalPitr/map_reduce/pkg/config"
	"github.com/MichalPitr/map_reduce/pkg/interfaces"
)

func BenchmarkMapper(b *testing.B) {
	cfg := NewTestConfig()
	cfg.InputDir = "/mnt/input/"
	cfg.OutputDir = "/mnt/benchmark/"
	cfg.FileRange = "book-0-80"
	cfg.Mapper = &WordCounter{wordRegex: regexp.MustCompile(`\b\w+\b`)}

	// Determines the number of partitions
	cfg.NumReducers = 2
	Run(cfg)
}

type WordCounter struct {
	wordRegex *regexp.Regexp
}

func (wc *WordCounter) Map(input interfaces.MapInput, emit func(key, value string)) {
	text := input.Value()
	text = strings.ToLower(text)
	words := wc.wordRegex.FindAllString(text, -1)
	for _, word := range words {
		emit(word, "1")
	}
}

func NewTestConfig() *config.Config {
	cfg := config.Config{}
	return &cfg
}
