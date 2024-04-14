package main

import (
	"log"
	"regexp"
	"strings"

	"github.com/MichalPitr/map_reduce/pkg/config"
	"github.com/MichalPitr/map_reduce/pkg/interfaces"
	"github.com/MichalPitr/map_reduce/pkg/mapper"
	"github.com/MichalPitr/map_reduce/pkg/mapreduce"
)

type WordCounter struct{}

func (wc *WordCounter) Map(input interfaces.MapInput, emit func(key, value string)) {
	text := input.Value()
	wordRegex := regexp.MustCompile(`\b\w+\b`)

	text = strings.ToLower(text)
	words := wordRegex.FindAllString(text, -1)
	for _, word := range words {
		emit(word, "1")
	}
}

func main() {
	cfg := config.ParseFlags()
	log.Printf("cfg: %v", cfg)
	cfg.NumReducers = 2
	cfg.NumMappers = 2
	mapper.RegisterMapper(cfg, "WordCounter", func() interfaces.Mapper { return &WordCounter{} })
	mapreduce.Execute(cfg)
}
