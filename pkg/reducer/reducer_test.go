package reducer

import (
	"log"
	"strconv"
	"testing"

	"github.com/MichalPitr/map_reduce/pkg/config"
	"github.com/MichalPitr/map_reduce/pkg/interfaces"
)

func BenchmarkReducer(b *testing.B) {
	cfg := NewTestConfig()
	cfg.ReducerId = 1
	cfg.JobId = "job-2024-04-21-01-07-50"
	cfg.Reducer = &Adder{}
	cfg.NfsPath = "/mnt/"

	// Determines the number of partitions
	Run(cfg)
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

func NewTestConfig() *config.Config {
	cfg := config.Config{}
	return &cfg
}
