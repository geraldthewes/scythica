package sdsmeta

import (
	"fmt"
	"os"
	"testing"
	//"github.com/pranavraja/zen"
)

func TestOutput(t *testing.T) {
	cfg := Sdsmeta{
		Columns: []Sdscolumndef{{"dt", "Date", "partition"}, {"value", "character", ""}},
		Keyspace: Sdskeyspace{
			Key_size: 8192,
			Nodes:    2,
			Rows:     1000}}
	output, err := OutputYAMLConfiguration(&cfg)
	if err != nil {
		panic(err)
	}
	s := string(output)
	//fmt.Printf("%d\n", len(s))
	fmt.Printf(s)
}

func TestInput(t *testing.T) {
	yaml := `columns:
- colname: dt
  coltype: Date
  attributes: partition
- colname: value
  coltype: character
keyspace:
  key_size: 8192
  nodes: 2
  rows: 1000`

	cfg, err := ReadYAMLConfiguration(yaml)
	if err != nil {
		panic(err)
	}

	output, err := OutputYAMLConfiguration(&cfg)
	if err != nil {
		panic(err)
	}
	s := string(output)
	fmt.Printf(s)

	var file *os.File
	file, err = os.Create("test-out.yaml")
	if err != nil {
		panic(err)
	}
	//var n int
	_, err = file.Write(output)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

}

func TestRead(t *testing.T) {
	_, err := ReadYAMLConfigurationFromFile("test-out.yaml")
	if err != nil {
		panic(err)
	}

}
