package scythica

import (
	"fmt"
	"testing"
	//"github.com/pranavraja/zen"
)

func TestOutput(t *testing.T) {
	cfg := Sdsmeta{
		Columns: []Sdscolumndef{{"Date", "dt"}, {"character", "value"}},
		Keyspace: Sdskeyspace{
			Key_size:    8192,
			Nodes:       2,
			Partitionby: "dt"}}
	output, err := WriteYAMLConfiguration(&cfg)
	if err != nil {
		panic(err)
	}
	s := string(output)
	//fmt.Printf("%d\n", len(s))
	fmt.Printf(s)
}

func TestInput(t *testing.T) {
	yaml := `columns:
- colname: Date
  coltype: dt
- colname: character
  coltype: value
keyspace:
  key_size: 8192
  nodes: 2
  partitionby: dt`

	cfg, err := ReadYAMLConfiguration(yaml)
	if err != nil {
		panic(err)
	}

	output, err := WriteYAMLConfiguration(&cfg)
	s := string(output)
	fmt.Printf(s)

}
