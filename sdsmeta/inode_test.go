package sdsmeta

import (
	"fmt"
	. "github.com/pranavraja/zen"
	"os"
	"testing"
)

var cfgsample = Sdsmeta{
	Columns: []Sdscolumndef{{"dt", "Date", PKEY}, {"value", "character", ""}},
	Keyspace: Sdskeyspace{
		Key_size: 8192,
		Nodes:    2,
		Rows:     1000}}

func TestOutput(t *testing.T) {
	output, err := OutputYAMLConfiguration(&cfgsample)
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
  attributes: pkey
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

func TestColTypes(t *testing.T) {
	var bcfg = Sdsmeta{
		Columns: []Sdscolumndef{{"year", "int32", PKEY},
			{"month", "int32", PKEY},
			{"day", "bogus", PKEY},
			{"value", "character", ""}},
		Keyspace: Sdskeyspace{
			Key_size: 8192,
			Nodes:    2,
			Rows:     1000},
		NCols: 4}

	Desc(t, "Column Types", func(it It) {

		it("check valid", func(expect Expect) {
			pos := cfg.verifyColumnTypes()
			expect(pos).ToEqual(0)
		})

		it("check invalid", func(expect Expect) {
			pos := bcfg.verifyColumnTypes()
			expect(pos).ToEqual(2)
		})

	})
}

func TestDFMeta(t *testing.T) {
	Desc(t, "data freame meta functions", func(it It) {

		sdf := SDataFrame{
			Schema:         cfg,
			Location:       "",
			partitionIndex: nil}

		it("ncol", func(expect Expect) {
			n := sdf.Ncol()
			expect(n).ToEqual(2)
		})

	})
}

func TestPartitions(t *testing.T) {
	Desc(t, "partitions simple", func(it It) {

		sdf := SDataFrame{
			Schema:         cfg,
			Location:       "",
			partitionIndex: nil}

		it("create index", func(expect Expect) {
			pkeys := sdf.createPartitionIndex()
			fmt.Printf("pkeys count: (%d)\n", pkeys)
			expect(pkeys).ToEqual(1)
		})

		it("check index", func(expect Expect) {
			expect(len(sdf.partitionIndex)).ToEqual(1)
		})
		it("check index at 0", func(expect Expect) {
			expect(sdf.partitionIndex[0]).ToEqual(0)
		})
	})

}

func TestPartitionsMulti(t *testing.T) {
	Desc(t, "partitions multi", func(it It) {

		var mcfg = Sdsmeta{
			Columns: []Sdscolumndef{{"year", "integer", PKEY},
				{"month", "integer", PKEY},
				{"day", "integer", PKEY},
				{"value", "character", ""}},
			Keyspace: Sdskeyspace{
				Key_size: 8192,
				Nodes:    2,
				Rows:     1000}}

		sdf := SDataFrame{
			Schema:         mcfg,
			Location:       "",
			partitionIndex: nil}

		it("create index", func(expect Expect) {
			pkeys := sdf.createPartitionIndex()
			fmt.Printf("pkeys count: (%d)\n", pkeys)
			expect(pkeys).ToEqual(3)
		})

		it("check index", func(expect Expect) {
			expect(len(sdf.partitionIndex)).ToEqual(3)
		})
		it("check index at 0", func(expect Expect) {
			expect(sdf.partitionIndex[0]).ToEqual(0)
		})
		it("check index at 1", func(expect Expect) {
			expect(sdf.partitionIndex[1]).ToEqual(1)
		})
		it("check index at 2", func(expect Expect) {
			expect(sdf.partitionIndex[2]).ToEqual(2)
		})
	})

}
