package main

import (
	//"fmt"
	"github.com/geraldthewes/scythica/sdsmeta"
	//"os"
	"testing"
)

const DATA_CFG = "data/airline.yaml"

func TestCreate(t *testing.T) {
	// read configuration
	cfg, err := sdsmeta.ReadYAMLConfigurationFromFile(DATA_CFG)
	if err != nil {
		panic(err)
	}
	err = createFromCsv(cfg, "test-out", "input")
	if err != nil {
		panic(err)
	}

}
