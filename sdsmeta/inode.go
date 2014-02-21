package sdsmeta

import (
	//"fmt"
	"launchpad.net/goyaml"
	"os"
)

type Sdskeyspace struct {
	Key_size    int32
	Nodes       int32
	Rows        int32
	Partitionby string
}

type Sdscolumndef struct {
	Colname string
	Coltype string
}

type Sdsmeta struct {
	Columns  []Sdscolumndef
	Keyspace Sdskeyspace
}

const max_cfg = 8192

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

// Create a New SDS File.
// Requires the directory to be empty
func CreateSDataSet(cfgMeta *Sdsmeta, location string) (err error) {

	// Create Top Level Directory
	err = os.Mkdir(location, 0774)
	if err != nil {
		return err
	}

	//

	return nil

}
