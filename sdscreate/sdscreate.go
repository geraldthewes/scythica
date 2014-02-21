package main

import (
	"flag"
	"fmt"
	"github.com/geraldthewes/scythica/sdsmeta"
	"os"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s schema_conf location < data \n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		usage()
	}
	conf := args[0]
	location := args[1]

	// read configuration
	cfg, err := sdsmeta.ReadYAMLConfigurationFromFile(conf)
	if err != nil {
		panic(err)
	}

	err = sdsmeta.CreateSDataSet(&cfg, location)

}
