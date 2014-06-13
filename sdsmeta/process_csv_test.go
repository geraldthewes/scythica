package sdsmeta

import (
	"fmt"
	. "github.com/pranavraja/zen"
	"os"
	"testing"
)

const DATA_CFG = "../data/airline.yaml"
const DATA_DATA = "../data/airline.csv"
const DATADS = "airline"

type progresstty struct {
}

func (p progresstty) Progress(pkey string, rows int32) {
	fmt.Printf("Created partition %s rows: %d\n", pkey, rows)
}

func TestCreate(t *testing.T) {
	Desc(t, "TestCreate Airline CSV\n", func(it It) {

		// Delete existing directory if necessary
		_ = os.RemoveAll(DATADS)

		// read configuration
		cfg, err := ReadYAMLConfigurationFromFile(DATA_CFG)
		if err != nil {
			panic(err)
		}

		p := progresstty{}
		err = CreateFromCsv(cfg, DATADS, DATA_DATA, p, false, false)
		if err != nil {
			panic(err)
		}

		it("check ncols", func(expect Expect) {
			expect(cfg.NCols).ToEqual(29)
		})
	})

}

const IRIS_DATA_CFG = "../data/iris.yaml"
const IRIS_DATA_DATA = "../data/iris.data"
const IRIS_DATADS = "iris"

func TestCreateIris(t *testing.T) {
	Desc(t, "TestCreate Iris CSV\n", func(it It) {

		// Delete existing directory if necessary
		_ = os.RemoveAll(IRIS_DATADS)

		// read configuration
		cfg, err := ReadYAMLConfigurationFromFile(IRIS_DATA_CFG)
		if err != nil {
			panic(err)
		}

		p := progresstty{}
		err = CreateFromCsv(cfg, IRIS_DATADS, IRIS_DATA_DATA, p, false, true)
		if err != nil {
			panic(err)
		}

		it("check ncols", func(expect Expect) {
			expect(cfg.NCols).ToEqual(5)
		})
	})

}
