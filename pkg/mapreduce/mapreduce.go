package mapreduce

import (
	"log"
	"os"

	"github.com/MichalPitr/map_reduce/pkg/config"
	"github.com/MichalPitr/map_reduce/pkg/mapper"
	"github.com/MichalPitr/map_reduce/pkg/master"
	"github.com/MichalPitr/map_reduce/pkg/reducer"
)

func Execute(cfg *config.Config) {
	switch cfg.Mode {
	case "master":
		master.Run(cfg)
	case "mapper":
		mapper.Run(cfg)
	case "reducer":
		reducer.Run(cfg)
	default:
		log.Printf("Invalid mode specified: %q", cfg.Mode)
		os.Exit(128)
	}
}
