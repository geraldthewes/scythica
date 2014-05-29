package main

import (
	"flag"
	"fmt"
	"github.com/geraldthewes/scythica/sdsmeta"
	"os"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [-noappend] schema_conf location csv_data \n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(2)
}

type importprogress struct {
}

func (p importprogress) Progress(pkey string, rows int32) {
	if rows > 0 {
		fmt.Printf("Created partition %s rows: %d\n", pkey, rows)
	}
}

var noappend bool

func init() {
	flag.BoolVar(&noappend, "noappend", false, "set to true to skip abort on append")
}

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) < 3 {
		usage()
	}
	conf := args[0]
	location := args[1]
	csvFile := args[2]

	// read configuration
	schema, err := sdsmeta.ReadYAMLConfigurationFromFile(conf)
	if err != nil {
		panic(err)
	}

	p := importprogress{}
	err = sdsmeta.CreateFromCsv(schema, location, csvFile, p, noappend)
	if err != nil {
		panic(err)
	}

}
