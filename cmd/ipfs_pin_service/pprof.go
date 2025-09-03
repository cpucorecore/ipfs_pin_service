package main

import (
	"fmt"
	"github.com/cpucorecore/ipfs_pin_service/log"
	"net/http"
)

func StartPprof(pprofPort int) {
	go func() {
		pprofAddr := fmt.Sprintf(":%d", pprofPort)
		log.Log.Sugar().Infof("Starting pprof server on %s", pprofAddr)
		if err := http.ListenAndServe(pprofAddr, nil); err != nil {
			log.Log.Sugar().Errorf("pprof server error: %v", err)
		}
	}()
}
