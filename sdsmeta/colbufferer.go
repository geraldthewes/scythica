package sdsmeta

type columnBufferer interface {
	String() (s string)
	setCol(row int32, value string) (err error)
	flushToDisk(rows int32, split int32) (err error)
}
