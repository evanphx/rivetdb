package sstable

func EstimateMemory(key, val []byte) int {
	// A casual estimate of the memory used to store this pair.
	// we store the key twice, once in the index and once in the entry,
	// then we have the average for the offset and version twice,
	// finally the length of the val itself, then the overhead
	// to store the length of key and val themselves.
	return (len(key) * 2) + 4 + 4 + 4 + len(val) + 8
}
