package sdsmeta

import (
	"encoding/csv"
	"os"
)

// Check Header matches
func matchHeader() (match bool) {
	return true
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

	// Read data
	for {
		//var row []string
		_, err = csvReader.Read()
		if err != nil {
			return err
		}

		// Extract partition key
	}

	return nil

}
