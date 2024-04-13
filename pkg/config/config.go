package config

import "flag"

type Config struct {
	Mode      string
	InputDir  string
	OutputDir string
	FileRange string
}

func ParseFlags() *Config {
	cfg := &Config{}
	// Common flags
	flag.StringVar(&cfg.Mode, "mode", "", "Mode of operation: master, mapper, reducer.")
	flag.StringVar(&cfg.InputDir, "input-dir", "", "Path to input directory.")

	// Mapper and reducer flags
	flag.StringVar(&cfg.FileRange, "file-range", "", "File ranges of files to be processed. Expected format `prefix-start-end`")
	flag.StringVar(&cfg.OutputDir, "output-dir", "", "Path to output directory.")
	flag.Parse()
	return cfg
}
