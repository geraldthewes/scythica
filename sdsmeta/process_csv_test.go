package sdsmeta

import (
	"fmt"
	"os"
	"testing"
)

const DATA_CFG = "../sdscreate/data/airline.yaml"
const DATA_DATA = "../sdscreate/data/airline.csv"
const DATADS = "airline"

type progresstty struct {
}

func (p progresstty) Progress(pkey string, rows int) {
	fmt.Printf("Created partition %s rows: %d\n", pkey, rows)
}

func TestCreate(t *testing.T) {
	fmt.Printf("TestCreate  CSV\n")

	// Delete existing directory if necessary
	_ = os.RemoveAll(DATADS)

	// read configuration
	cfg, err := ReadYAMLConfigurationFromFile(DATA_CFG)
	if err != nil {
		panic(err)
	}

	p := progresstty{}
	err = CreateFromCsv(cfg, DATADS, DATA_DATA, p)
	if err != nil {
		panic(err)
	}

}
