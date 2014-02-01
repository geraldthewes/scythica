package scythica

import (
	"launchpad.net/goyaml"
	"os"
)

type sdskeyspace struct {
	key_size  int32
	nodes     int32
	partionby string
}

type sdscolumndef struct {
	colname string
	coltype string
}

type sdsmeta struct {
	columns  []sdscolumndef
	keyspace sdskeyspace
}

// Read data set configuration information
func readConfiguration(cfgFile string) {
	// Load configuration file in memory
	file, err := os.Open(cfgFile)
	if err != nil {
		//log.Fatal(err)
		panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	buf := make([]byte, 1024)
	n, err := file.Read(buf)

	if n >= 1024 {
		panic("Configuration file too large")
	}

	var cfg sdsmeta
	err = goyaml.Unmarshal(buf, &cfg)
	if err != nil {
		//log.Fatal(err)
		panic(err)
	}
}
