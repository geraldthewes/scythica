package sdsmeta

type partitionStorer interface {
	close() error
	put(key string, value int64) error
}
