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
		_, err = CreateDataframeFromCsv(cfg, 
			DATADS, 
			DATA_DATA, 
			p, 
			false, 
			false,
			"")
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
		_, err = CreateDataframeFromCsv(cfg, 
			IRIS_DATADS, 
			IRIS_DATA_DATA, 
			p, 
			false, 
			true,
			"")
		if err != nil {
			panic(err)
		}

		it("check ncols", func(expect Expect) {
			expect(cfg.NCols).ToEqual(5)
		})
	})

}

const IRIS_DATA_TAB = "../data/iris.tab"
const IRIS_DATADS_TAB = "iris.tab"

func TestCreateIrisTab(t *testing.T) {
	Desc(t, "TestCreate Iris Tab Delimited\n", func(it It) {

		// Delete existing directory if necessary
		_ = os.RemoveAll(IRIS_DATADS_TAB)

		// read configuration
		cfg, err := ReadYAMLConfigurationFromFile(IRIS_DATA_CFG)
		if err != nil {
			panic(err)
		}

		p := progresstty{}
		_, err = CreateDataframeFromCsv(cfg, 
			IRIS_DATADS_TAB, 
			IRIS_DATA_TAB, 
			p, 
			false, 
			true,
			"\t")
		if err != nil {
			panic(err)
		}

		it("check ncols", func(expect Expect) {
			expect(cfg.NCols).ToEqual(5)
		})
	})

}


const BOS_DATA_CFG = "../data/boston.yaml"
const BOS_DATA_DATA = "../data/boston-1970-2014.csv"
const BOS_DATADS = "boston"


func TestCreateBoston(t *testing.T) {
	Desc(t, "Test Boston CSV with dates\n", func(it It) {

		// Delete existing directory if necessary
		_ = os.RemoveAll(BOS_DATADS)

		// read configuration
		cfg, err := ReadYAMLConfigurationFromFile(BOS_DATA_CFG)
		if err != nil {
			panic(err)
		}

		p := progresstty{}
		_, err = CreateDataframeFromCsv(cfg, 
			BOS_DATADS, 
			BOS_DATA_DATA, 
			p, 
			false, 
			false,
			"")
		if err != nil {
			panic(err)
		}

		it("check ncols", func(expect Expect) {
			expect(cfg.NCols).ToEqual(4)
		})
	})

}


