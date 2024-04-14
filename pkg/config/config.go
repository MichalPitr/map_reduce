package config

import "flag"

type Config struct {
	Mode         string
	InputDir     string
	OutputDir    string
	FileRange    string
	MapperClass  string
	ReducerClass string
	NumReducers  int
	NumMappers   int
}

func ParseFlags() *Config {
	cfg := &Config{}
	// Common flags
	flag.StringVar(&cfg.Mode, "mode", "", "Mode of operation: master, mapper, reducer.")
	flag.StringVar(&cfg.InputDir, "input-dir", "", "Path to input directory.")
	flag.IntVar(&cfg.NumReducers, "num-reducers", 1, "Number of reducers to use.")
	flag.IntVar(&cfg.NumMappers, "num-mappers", 1, "Number of mappers to use.")
	// Mapper and reducer flags
	flag.StringVar(&cfg.FileRange, "file-range", "", "File ranges of files to be processed. Expected format `prefix-start-end`")
	flag.StringVar(&cfg.OutputDir, "output-dir", "", "Path to output directory.")
	flag.Parse()
	return cfg
}
