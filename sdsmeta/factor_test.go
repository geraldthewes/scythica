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

		it("new factor 1", func(expect Expect) {
			expect(f.encode("One")).ToEqual(1)
		})

		it("lookup factor 1", func(expect Expect) {
			expect(f.lookup("One")).ToEqual(1)
		})

		it("get factor 1", func(expect Expect) {
			expect(f.get(1)).ToEqual("One")
		})

		it("new factor 2", func(expect Expect) {
			expect(f.encode("Two")).ToEqual(2)
		})

		it("new factor 1", func(expect Expect) {
			expect(f.encode("One")).ToEqual(1)
		})

		it("check length", func(expect Expect) {
			expect(f.length()).ToEqual(2)
		})

	})

}

func TestFactorIO(t *testing.T) {
	Desc(t, "TestFactors\n", func(it It) {

		var f factor
		f.init()

		f.encode("One")
		f.encode("Two")
		f.encode("Three")

		it("save factor", func(expect Expect) {
			expect(f.save("factor.db")).ToNotExist()
		})

		it("check length", func(expect Expect) {
			expect(f.length()).ToEqual(3)
		})

		var g factor
		g.init()

		it("load factor", func(expect Expect) {
			expect(g.load("factor.db")).ToNotExist()
		})

		it("check length", func(expect Expect) {
			expect(g.length()).ToEqual(3)
		})

		it("get factor 1", func(expect Expect) {
			expect(g.get(1)).ToEqual("One")
		})
		it("lookup factor 1", func(expect Expect) {
			expect(g.lookup("One")).ToEqual(1)
		})

		it("get factor 2", func(expect Expect) {
			expect(g.get(2)).ToEqual("Two")
		})
		it("lookup factor 2", func(expect Expect) {
			expect(g.lookup("Two")).ToEqual(2)
		})

		it("get factor 3 ", func(expect Expect) {
			expect(g.get(3)).ToEqual("Three")
		})
		it("lookup factor 3", func(expect Expect) {
			expect(g.lookup("Three")).ToEqual(3)
		})

	})

}
