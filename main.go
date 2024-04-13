package main

import (
	"strings"

	"github.com/MichalPitr/map_reduce/pkg/config"
	"github.com/MichalPitr/map_reduce/pkg/interfaces"
	"github.com/MichalPitr/map_reduce/pkg/mapper"
	"github.com/MichalPitr/map_reduce/pkg/mapreduce"
)

type WordCounter struct{}

func (wc *WordCounter) Map(input interfaces.MapInput, emit func(key, value string)) {
	text := input.Value()
	words := strings.Fields(text)
	for _, word := range words {
		emit(word, "1")
	}
}

func main() {
	cfg := config.ParseFlags()
	mapper.RegisterMapper(cfg, "WordCounter", func() interfaces.Mapper { return &WordCounter{} })
	mapreduce.Execute(cfg)
}
