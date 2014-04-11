package sdsmeta

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

// Created SDS Dataframe from CSV file
func CreateFromCsv(schema Sdsmeta, location string, csvFile string) (err error) {
	var df SDataFrame
	df.Schema = schema
	df.Location = location

	err = CreateSDataSet(&df.Schema, location)
	if err != nil {
		return err
	}

	err = LoadCsv(&df, csvFile)
	return err

}

// Check Header matches
func matchHeader() (match bool) {
	return true
}

// Create Partition label
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

// Load CSV file in df Data Frame
func LoadCsv(df *SDataFrame, csvFileName string) (err error) {
	// Assume partitions are contiguous
	// Iterate over every row
	// If partition changes - start new partition
	// If number of rows exceeded, start new bank
	// Write each column

	var csvFile *os.File

	err = nil
	csvFile, err = os.Open(csvFileName)
	if err != nil {
		return err
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)

	// Read header
	//var header []string
	_, err = csvReader.Read()
	if err != nil {
		return err
	}
	matchHeader()
	df.createPartitionIndex()

	// Read data
	pkey := "-nil-"
	var buffers SDataFramePartitionCols
	buffers.Rows = 0

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
			err = buffers.FlushToDisk()
			if err != nil {
				return err
			}

			pkey = npkey
			buffers, err = df.CreatePartition(pkey)
		}

		err = buffers.setRow(buffers.Rows, record)
		if err != nil {
			return err
		}

		buffers.Rows++

	}
	fmt.Printf("Flush\n")
	err = buffers.FlushToDisk()

	return err

}
