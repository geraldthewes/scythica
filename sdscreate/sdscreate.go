/*
This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.
*/

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
var noheader bool

func init() {
	flag.BoolVar(&noappend, "noappend", false, "set to true to skip abort on append")
	flag.BoolVar(&noheader, "noheader", false, "set to true if data file has no header")
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
	_, err = sdsmeta.CreateDataframeFromCsv(schema,
		location,
		csvFile,
		p,
		noappend,
		noheader)
	if err != nil {
		panic(err)
	}

}
