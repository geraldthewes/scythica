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
	//	"fmt"
	"launchpad.net/goyaml"
	"os"
	"strings"
)

// Dataframe keyspace attributes
type Sdskeyspace struct {
	Key_size       int32  // Logical size of hashing key ring
	Nodes          int32  // Number of physical nodes
	Rows_per_split int32  // Number of Rows per split
	IsNA           string "isna,omitempty" // Value that acts as NA
}

// Column definition
type Sdscolumndef struct {
	Colname    string
	Coltype    string
	Attributes string "attributes,omitempty"
}

// Dataset Schema and attributes
type Sdsmeta struct {
	Columns  []Sdscolumndef
	Keyspace Sdskeyspace
	NCols    int "ncols,omitempty"
}

type SError struct {
	msg string
}

// Max Configuration File Size
const max_cfg = 8192
const PKEY = "pkey"
const PKEY_0PAD2 = "pkey0p2"

const SDF_Integer32 = "int32"
const SDF_Float = "float"
const SDF_Double = "double"
const SDF_Date = "date"
const SDF_DateTime = "datetime"
const SDF_Character = "character"
const SDF_Factor = "factor"
const SDF_Boolean = "boolean"
const SDF_Integer64 = "int64"

const SDFK_Integer32 = 1
const SDFK_Float = 2
const SDFK_Double = 3
const SDFK_Date = 4
const SDFK_Character = 5
const SDFK_Factor = 6
const SDFK_Boolean = 7
const SDFK_Integer64 = 8
const SDFK_DateTime = 9

var SDF_ColType_Keywords = map[string]int{
	SDF_Integer32: SDFK_Integer32,
	SDF_Float:     SDFK_Float,
	SDF_Double:    SDFK_Double,
	SDF_Date:      SDFK_Date,
	SDF_DateTime:  SDFK_DateTime,
	SDF_Character: SDFK_Character,
	SDF_Factor:    SDFK_Factor,
	SDF_Boolean:   SDFK_Boolean,
	SDF_Integer64: SDFK_Integer64}

const DF_SCHEMA = "/schema.cfg"
const DF_DATA_DIR = "/data"
const DF_FACTORS_DIR = "/factors"
const DF_SEP = "/"
const DF_FS = "-"
const DF_PDB = "pdb.key"
const DB_NROW = "nrow"

func (e *SError) Error() string {
	return e.msg
}

// Read data set configuration information from string
func ReadYAMLConfiguration(cfgstring string) (cfg Sdsmeta, err error) {
	buf := []byte(cfgstring)
	err = goyaml.Unmarshal(buf, &cfg)
	if err != nil {
		//log.Fatal(err)
		panic(err)
	}
	cfg.NCols = len(cfg.Columns)
	return cfg, err
}

// Read data set configuration information from file
func ReadYAMLConfigurationFromFile(cfgFile string) (cfg Sdsmeta, err error) {
	// Load configuration file in memory
	file, err := os.Open(cfgFile)
	// !!! Possibly should use ioutil
	if err != nil {
		//log.Fatal(err)
		panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	buf := make([]byte, max_cfg)
	n, err := file.Read(buf)

	if n >= max_cfg {
		panic("Configuration file too large")
	}

	buf2 := buf[0:n]
	//fmt.Printf("ReadYAMLConfigurationFromFile\n")
	//fmt.Printf("%d\n", n)
	//for i := 0; i < len(buf2); i++ {
	//	fmt.Printf("%x ", buf2[i])
	//}
	//fmt.Printf(string(buf2))

	err = goyaml.Unmarshal(buf2, &cfg)
	if err != nil {
		//log.Fatal(err)
		panic(err)
	}
	cfg.NCols = len(cfg.Columns)
	return cfg, err
}

// Write table configuration file
func OutputYAMLConfiguration(cfgMeta *Sdsmeta) (out []byte, err error) {
	out, err = goyaml.Marshal(cfgMeta)
	return out, err
}

// Write configuration file to file
func WriteYAMLConfigurationToFile(cfgMeta *Sdsmeta, outFile string) (err error) {
	var file *os.File
	file, err = os.Create(outFile)
	if err != nil {
		return err
	}

	defer func() {
		if err = file.Close(); err != nil {
			// should log something return err
		}
	}()

	var output []byte
	output, err = OutputYAMLConfiguration(cfgMeta)
	if err != nil {
		return err
	}

	_, err = file.Write(output)
	if err != nil {
		return err
	}

	return nil
}

// Is the column a key column?
func (coldef *Sdscolumndef) isPartOfKey() (ret bool) {
	if strings.Contains(coldef.Attributes, PKEY) {
		return true
	}
	return false
}

// Verify Column Types
func (sdm *Sdsmeta) verifyColumnTypes() (pos int) {

	for index, element := range sdm.Columns {
		if SDF_ColType_Keywords[element.Coltype] == 0 {
			return index
		}
	}

	return -1 // No error
}
