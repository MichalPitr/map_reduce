package main

import (
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/MichalPitr/map_reduce/pkg/config"
	"github.com/MichalPitr/map_reduce/pkg/interfaces"
	"github.com/MichalPitr/map_reduce/pkg/mapreduce"
)

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

type Adder struct{}

func (a *Adder) Reduce(input interfaces.ReducerInput, emit func(value string)) {
	val := 0
	for !input.Done() {
		num, err := strconv.Atoi(input.Value())
		if err != nil {
			log.Printf("Failed converting input to integer, skipping: %s", input.Value())
			input.NextValue()
			continue
		}
		val += num
		input.NextValue()
	}
	emit(strconv.Itoa(val))
}

func main() {
	cfg := config.SetupJobConfig()
	log.Printf("cfg: %v", cfg)
	cfg.NumReducers = 2
	cfg.NumMappers = 2

	cfg.Mapper = &WordCounter{wordRegex: regexp.MustCompile(`\b\w+\b`)}
	cfg.Reducer = &Adder{}

	mapreduce.Execute(cfg)
}
