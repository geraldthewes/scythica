package sdsmeta

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
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

	LoadCsv(&df, csvFile)
	return nil

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
		buffer.WriteString(row[col])
		fmt.Printf("(%d,%s)", col, row[col])
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
	for {
		var row []string
		row, err = csvReader.Read()
		if err != nil {
			return err
		}

		// Extract partition key
		pkey := createPartitionLabel(df, row)
		fmt.Printf("pkey=%s\n", pkey)
	}

	return nil

}
