package sdsmeta

import (
	"fmt"
	"launchpad.net/goyaml"
	"os"
	"strings"
)

type Sdskeyspace struct {
	Key_size     int32
	Nodes        int32
	Rows         int32
	PartitionFmt string "partionfmt,omitempty"
}

type Sdscolumndef struct {
	Colname    string
	Coltype    string
	Attributes string "attributes,omitempty"
}

// Hold information on Scythica Data Set
type Sdsmeta struct {
	Columns  []Sdscolumndef
	Keyspace Sdskeyspace
}

// Hold Runtime Information
type SDataFrame struct {
	Schema         Sdsmeta
	Location       string
	partitionIndex []int // index of all partition keys
	typeIndex      []int // index in type
}

// Single Buffer for a Partition of a Column
type SDataFrameColBuffer struct {
	Column           Sdscolumndef
	Path             string
	PartitionKey     string
	DataBufferInt32  []int32
	DataBufferFloat  []float32
	DataBufferDouble []float64
	DataBufferInt64  []int64
	DataBufferByte   []byte
	DataBufferFactor []int32
	DataBufferBool   []bool
}

type SDataFramePartitionCols struct {
	colBuffers []SDataFrameColBuffer
}

type SError struct {
	msg string
}

// Max Configuration File Size
const max_cfg = 8192
const PKEY = "pkey"
const PKEY_0PAD2 = "pkey0p2"

const SDF_Integer = "integer"
const SDF_Float = "float"
const SDF_Double = "double"
const SDF_Date = "date"
const SDF_Character = "character"
const SDF_Factor = "factor"
const SDF_Boolean = "boolean"
const SDF_Integer64 = "int64"

var SDF_ColType_Keywords = map[string]int{
	SDF_Integer:   1,
	SDF_Float:     2,
	SDF_Double:    3,
	SDF_Date:      4,
	SDF_Character: 5,
	SDF_Factor:    6,
	SDF_Boolean:   7,
	SDF_Integer64: 8}

const DF_SCHEMA = "/schema.cfg"
const DF_DATA_DIR = "/data"
const DF_SEP = "/"

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

// Create a New Empty SDS File.
// Requires the directory to be empty
func CreateSDataSet(schema *Sdsmeta, location string) (err error) {

	pos := schema.verifyColumnTypes()
	if pos >= 0 {
		var e SError
		e.msg = fmt.Sprintf("Invalid column type for %s in position %d",
			schema.Columns[pos].Colname,
			pos)
		return &e // $$$ Does this really work?
	}

	// Create Top Level Directory
	err = os.Mkdir(location, 0774)
	if err != nil {
		return err
	}

	// Save configuration file
	cfgFile := location + DF_SCHEMA
	err = WriteYAMLConfigurationToFile(schema, cfgFile)
	if err != nil {
		return err
	}

	// Create data subdirectory
	dataDir := location + DF_DATA_DIR
	err = os.Mkdir(dataDir, 0774)
	if err != nil {
		return err
	}

	return nil
}

// Create list of offsets for partition
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

// Verify Column Types
func (sdm *Sdsmeta) verifyColumnTypes() (pos int) {

	for index, element := range sdm.Columns {
		if SDF_ColType_Keywords[element.Coltype] == 0 {
			return index
		}
	}

	return -1 // No error
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

// Allocate a column partition buffer
func (sdf *SDataFrame) AllocateColPartitionBuffer(col Sdscolumndef, pKey string) (colBuffer SDataFrameColBuffer) {
	colBuffer.PartitionKey = pKey
	colBuffer.Path = sdf.PartitionPath(pKey)
	nrows := sdf.Schema.Keyspace.Rows
	switch SDF_ColType_Keywords[col.Coltype] {
	case 1:
		fallthrough
	case 6:
		colBuffer.DataBufferInt32 = make([]int32, nrows)
	case 2:
		colBuffer.DataBufferFloat = make([]float32, nrows)
	case 3:
		colBuffer.DataBufferDouble = make([]float64, nrows)
	case 4:
		fallthrough
	case 8:
		colBuffer.DataBufferInt64 = make([]int64, nrows)
	case 5:
		colBuffer.DataBufferByte = make([]byte, nrows)
	case 7:
		colBuffer.DataBufferBool = make([]bool, nrows)
	default:
		panic("Unknown column type")
	}
	return
}

// Allocate all column partition buffers
func (sdf *SDataFrame) AllocateAllColsPartitionBuffer(pKey string) (buffers SDataFramePartitionCols) {

	for index, element := range sdf.Schema.Columns {
		buffers.colBuffers[index] = sdf.AllocateColPartitionBuffer(element, pKey)
	}
	return
}

// Create Partition
func (sdf *SDataFrame) CreatePartition(pkey string) (buffers SDataFramePartitionCols, err error) {
	err = os.Mkdir(sdf.PartitionPath(pkey), 0774)
	if err != nil {
		return buffers, err
	}
	buffers = sdf.AllocateAllColsPartitionBuffer(pkey)
	return buffers, nil

}
