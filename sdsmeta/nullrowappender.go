package sdsmeta

// Does nothing
type nullRowAppender struct {
}

func (pCols *nullRowAppender) FlushToDisk() (err error) {
	return nil
}

func (pCols *nullRowAppender) AppendRow(record []string) (err error) {
	return nil
}

func (pCols *nullRowAppender) Rows() (nrows int32) {
	return 0
}
