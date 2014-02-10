package sdsmeta

import (
	"os"
)

// Create an SDS file
func CreateSDS(cfg Sdsmeta, location string) (err error) {
	// Since we are creating a structure, fail if it exists
	err = os.Mkdir(location, 0744)
	if err != nil {
		return err
	}

	return nil
}
