package sdsmeta

import (
	"github.com/ugorji/go/codec"
	"io"
	"os"
)

type msgpckstore struct {
	mh  codec.MsgpackHandle
	fo  *os.File
	enc *codec.Encoder
}

func openMsgPackStore(path string) (mps msgpckstore, err error) {
	err = nil
	fname := path + DF_SEP + "db.mp"
	//fmt.Printf("write ... %s\n", fname)
	mps.fo, err = os.Create(fname)
	if err != nil {
		return
	}
	var w io.Writer
	w = io.Writer(mps.fo)
	mps.enc = codec.NewEncoder(w, &mps.mh)

	return
}

func (mps msgpckstore) close() (err error) {
	err = mps.fo.Close()
	return
}

func (mps msgpckstore) put(key string, value int64) (err error) {
	m := make(map[string]int64)
	m[key] = value

	err = mps.enc.Encode(m)

	return
}
