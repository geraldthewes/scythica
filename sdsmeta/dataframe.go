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
	"os"
	"strings"
)

// Main dataframe class, with access both to meta data and dataframe data
type SDataFrame struct {
	Schema         Sdsmeta
	Location       string
	partitionIndex []int // index of all partition keys
	typeIndex      []int // index in type
}

// New empty Dataframe from schema
func NewDataFrame(schema Sdsmeta, location string) (sdf SDataFrame) {
	sdf.Schema = schema
	sdf.Location = location
	return
}

// Create a New Empty dataframe file structure on disk
// Requires the directory to be empty
func (sdf *SDataFrame) CreateNewDataFrameOnDisk() (err error) {

	pos := sdf.Schema.verifyColumnTypes()
	if pos >= 0 {
		var e SError
		e.msg = fmt.Sprintf("Invalid column type for %s in position %d",
			sdf.Schema.Columns[pos].Colname,
			pos)
		return &e
	}

	// Create Top Level Directory
	err = os.Mkdir(sdf.Location, 0774)
	if err != nil {
		return err
	}

	// Save configuration file
	cfgFile := sdf.Location + DF_SCHEMA
	err = WriteYAMLConfigurationToFile(&sdf.Schema, cfgFile)
	if err != nil {
		return err
	}

	// Create data subdirectory
	dataDir := sdf.Location + DF_DATA_DIR
	err = os.Mkdir(dataDir, 0774)
	if err != nil {
		return err
	}

	return nil
}

// Create list of offsets for partition index for the dataframe. Aka, have list of columns that make
// part of the key
// Returns number of column that compose the index
func (sdf *SDataFrame) createPartitionIndex() (pkeys int) {
	pkeys = 0
	for _, element := range sdf.Schema.Columns {
		// Simple test for now
		if strings.Contains(element.Attributes, PKEY) {
			pkeys++
		}
	}

	// Now create the index
	i := 0
	sdf.partitionIndex = make([]int, pkeys)
	for index, element := range sdf.Schema.Columns {
		// Simple test for now
		if strings.Contains(element.Attributes, PKEY) ||
			strings.Contains(element.Attributes, PKEY_0PAD2) {
			sdf.partitionIndex[i] = index
			i++
		}
	}

	return pkeys
}

// return number of columns
func (sdf *SDataFrame) Ncol() int {
	return len(sdf.Schema.Columns)
}

// return path to partition
func (sdf *SDataFrame) PartitionPath(pKey string) (path string) {
	path = sdf.Location + DF_DATA_DIR + DF_SEP + pKey
	return
}

// Create a new Partition
func (sdf *SDataFrame) CreateNewPartition(buffers SDataFramePartitionCols, pkey string, noappend bool) (appender RowAppender, err error) {
	appender, err = buffers.CreatePartitionCols(sdf, pkey, noappend)
	return
}
