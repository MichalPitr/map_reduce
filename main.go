package main

import (
	"github.com/MichalPitr/map_reduce/pkg/config"
	"github.com/MichalPitr/map_reduce/pkg/mapreduce"
)

func main() {
	cfg := config.ParseFlags()
	mapreduce.Execute(cfg)
}
