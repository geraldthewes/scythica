package sdsmeta

import (
	//"fmt"
	"os"
	"testing"
)

const DATA_CFG = "../sdscreate/data/airline.yaml"
const DATA_DATA = "../sdscreate/data/airline.csv"
const DATADS = "airline"

func TestCreate(t *testing.T) {
	// Delete existing directory if necessary
	_ = os.RemoveAll(DATADS)

	// read configuration
	cfg, err := ReadYAMLConfigurationFromFile(DATA_CFG)
	if err != nil {
		panic(err)
	}
	err = CreateFromCsv(cfg, DATADS, DATA_DATA)
	if err != nil {
		panic(err)
	}

}
