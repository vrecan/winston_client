package hash

import (
	"stablelib.com/v1/crypto/siphash"
)

//Hash will hash a key to decide which partition the key should be sent to.
type Hash struct {
	Max int
}

//NewHash...
func NewHash() Hash {
	return Hash{Max: 2048}
}

//Hash the key returning the partition it should be sent to
func (h Hash) Hash(key string, partitions uint64) int {
	hash := siphash.Hash(0, 2048, []byte(key))
	partition := hash % partitions
	return int(partition)
}
