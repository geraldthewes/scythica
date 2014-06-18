/*
This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.
*/

package sdsmeta

import (
	//"fmt"
	. "github.com/pranavraja/zen"
	"testing"
)

func TestFactor(t *testing.T) {
	Desc(t, "TestFactors\n", func(it It) {

		var f factor
		f.init()

		it("new factor", func(expect Expect) {
			expect(f.encode("One")).ToEqual(1)
		})

		it("lookup factor", func(expect Expect) {
			expect(f.lookup("One")).ToEqual(1)
		})

		it("get factor 0", func(expect Expect) {
			expect(f.get(1)).ToEqual("One")
		})

		it("new factor", func(expect Expect) {
			expect(f.encode("Two")).ToEqual(2)
		})

		it("new factor", func(expect Expect) {
			expect(f.encode("One")).ToEqual(1)
		})

	})

}
