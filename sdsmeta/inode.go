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
	partitionIndex []int
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

var SDF_ColType_Keywords = map[string]int{
	SDF_Integer:   1,
	SDF_Float:     2,
	SDF_Double:    3,
	SDF_Date:      4,
	SDF_Character: 5,
	SDF_Factor:    6,
	SDF_Boolean:   7}

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
	cfgFile := location + "/schema.cfg"
	err = WriteYAMLConfigurationToFile(schema, cfgFile)
	if err != nil {
		return err
	}

	// Create data subdirectory
	dataDir := location + "/data"
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
