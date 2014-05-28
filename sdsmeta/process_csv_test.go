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
	Desc(t, "TestCreate  CSV\n", func(it It) {

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

		it("check ncols", func(expect Expect) {
			expect(cfg.NCols).ToEqual(29)
		})
	})

}
