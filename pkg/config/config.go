package config

import (
	"flag"

	"github.com/MichalPitr/map_reduce/pkg/interfaces"
)

type Config struct {
	Mode        string
	InputDir    string
	OutputDir   string
	FileRange   string
	NumReducers int
	NumMappers  int
	ReducerId   int
	NfsPath     string
	Image       string

	Mapper  interfaces.Mapper
	Reducer interfaces.Reducer
}

func SetupJobConfig() *Config {
	cfg := &Config{}
	// Common flags
	flag.StringVar(&cfg.Mode, "mode", "", "Mode of operation: master, mapper, reducer.")
	flag.StringVar(&cfg.InputDir, "input-dir", "", "Path to input directory.")
	flag.StringVar(&cfg.OutputDir, "output-dir", "", "Path to output directory.")
	flag.IntVar(&cfg.NumReducers, "num-reducers", 1, "Number of reducers to use.")
	flag.IntVar(&cfg.NumMappers, "num-mappers", 1, "Number of mappers to use.")
	flag.StringVar(&cfg.Image, "image", "", "Image with the binary.")

	// Mapper and reducer flags
	flag.StringVar(&cfg.NfsPath, "nfs-path", "/mnt/nfs", "Base directory where nfs is mounted.")
	flag.StringVar(&cfg.FileRange, "file-range", "", "File ranges of files to be processed. Expected format `prefix-start-end`")
	flag.IntVar(&cfg.ReducerId, "reducer-id", 0, "Reducer id.")
	flag.Parse()
	return cfg
}
