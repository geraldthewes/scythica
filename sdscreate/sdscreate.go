package main

import (
	"flag"
	"fmt"
	"github.com/geraldthewes/scythica/sdsmeta"
	"os"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s schema_conf location csv_data \n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(2)
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
	cfg, err := sdsmeta.ReadYAMLConfigurationFromFile(conf)
	if err != nil {
		panic(err)
	}

	err = createFromCsv(cfg, location, csvFile)
	if err != nil {
		panic(err)
	}

}

// Created SDS Dataframe from CSV file
func createFromCsv(cfg sdsmeta.Sdsmeta, location string, csvFile string) (err error) {
	var df sdsmeta.SDataFrame
	df.CfgFile = cfg
	df.Location = location

	err = sdsmeta.CreateSDataSet(&df.CfgFile, location)
	if err != nil {
		return err
	}

	sdsmeta.LoadCsv(&df, csvFile)
	return nil

}
