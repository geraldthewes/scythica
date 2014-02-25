package sdsmeta

//import "encoding/csv"

// Load CSV file in df Data Frame
func LoadCsv(df *SDataFrame, csvFile string) {
	// Assume partitions are contiguous
	// Iterate over every row
	// If partition changes - start new partition
	// If number of rows exceeded, start new bank
	// Write each column
}
