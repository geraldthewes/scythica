package sdsmeta

import (
	//"fmt"
	. "github.com/pranavraja/zen"
	//"os"
	"testing"
)

func TestColList(t *testing.T) {
	Desc(t, "SDataFramePartitionCols", func(it It) {

		var sdfpcs SDataFramePartitionCols

		it("String", func(expect Expect) {
			empty := sdfpcs.String()
			expect(empty).ToEqual("List of columns of length 0")
		})

	})

}
