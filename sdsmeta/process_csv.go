package sdsmeta

import (
	"bytes"
	"encoding/csv"
	//"fmt"
	"io"
	"os"
	"strings"
)

type ImportProgresser interface {
	Progress(pKey string, rows int32)
}

// Created SDS Dataframe from CSV file
func CreateFromCsv(schema Sdsmeta,
	location string,
	csvFile string,
	progress ImportProgresser,
	noappend bool) (err error) {
	var df = NewSDataFrame(schema, location)

	err = df.CreateSDataFrameOnDisk()
	if err != nil {
		return err
	}

	err = LoadCsv(&df, csvFile, progress, noappend)
	return err

}

// Check Header matches
func matchHeader() (match bool) {
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
func LoadCsv(df *SDataFrame,
	csvFileName string,
	progress ImportProgresser,
	noappend bool) (err error) {
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
			appender, err = df.CreatePartition(buffers, pkey, noappend)
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
