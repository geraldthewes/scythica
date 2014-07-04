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
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
"unicode/utf8"
)

type ImportProgresser interface {
	Progress(pKey string, rows int32)
}

// Created Dataframe from CSV file
func CreateDataframeFromCsv(schema Sdsmeta,
	location string,
	csvFile string,
	progress ImportProgresser,
	noappend bool,
	noheader bool,
        comma string) (df *SDataFrame, err error) {
	var sdf = NewDataFrame(schema, location)

	err = sdf.CreateNewDataFrameOnDisk()
	if err != nil {
		return nil, err
	}

	err = LoadFromCsv(sdf, csvFile, progress, noappend, noheader,comma)
	if err != nil {
		return nil, err
	}

	err = sdf.Close()

	return sdf, err

}

// Check Header matches
func matchHeader() (match bool) {
	// $$$ Not yet implemented
	return true
}

// Create partition label from all the key columns
func createPartitionLabel(sdf *SDataFrame, row []string) (label string) {
	// Count number of columns in the partition
	var buffer bytes.Buffer
	sep := ""
	for _, col := range sdf.partitionIndex {
		// How to append strings?
		buffer.WriteString(sep)
		if strings.Contains(sdf.Schema.Columns[col].Attributes, PKEY_0PAD2) {
			if len(row[col]) == 1 {
				buffer.WriteString("0")
			}
		}
		buffer.WriteString(row[col])

		//fmt.Printf("(%d,%s)", col, row[col])
		sep = "-"
	}
	// Then create the slice
	return buffer.String()
}

// Import CSV file in df Data Frame. Pass in an ImportProgresser to report on progress
// the progress of the import job.
// Pass in '-' in csvFileName to read from stdin
// set noappend to abort on a duplicate partition
// set noheader if file does not include headers
// if comma is not nil, use a seperator
func LoadFromCsv(df *SDataFrame,
	csvFileName string,
	progress ImportProgresser,
	noappend bool,
	noheader bool,
        comma string) (err error) {
	// Assume partitions are contiguous
	// Iterate over every row
	// If partition changes - start new partition
	// If number of rows exceeded, start new bank
	// Write each column

	var csvFile *os.File

	err = nil
	if csvFileName == "-" {
		csvFile = os.Stdin
	} else {
		csvFile, err = os.Open(csvFileName)
		if err != nil {
			return err
		}
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	if comma != "" {
		rc, pos := utf8.DecodeRuneInString(comma)
		fmt.Printf("Seperator is %#U starts at byte position %d\n", rc, pos)
		csvReader.Comma = rc
	}

	if !noheader {
		_, err = csvReader.Read()
		if err != nil {
			return err
		}
		matchHeader()
	}

	df.createPartitionIndex()

	// Read data
	pkey := "-nil-"

	var appender RowAppender
	// Will re-use same SDataFramePartionCols for each partition
	buffers := NewPartitionCols(df)
	appender = RowAppender(&buffers)

	for {
		var record []string

		record, err = csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Extract partition key and create partition if needed
		npkey := createPartitionLabel(df, record)
		//fmt.Printf("pkey=%s\n", npkey)
		if npkey != pkey {
			err = appender.FlushToDisk()
			if err != nil {
				return err
			}
			progress.Progress(pkey, appender.Rows())

			pkey = npkey
			appender, err = df.CreateNewPartition(buffers, pkey, noappend)
			if err != nil {
				return err
			}
		}

		err = appender.AppendRow(record)
		if err != nil {
			return err
		}

	}
	//fmt.Printf("Flush\n")
	err = appender.FlushToDisk()
	progress.Progress(pkey, appender.Rows())

	return err

}
