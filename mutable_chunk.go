package tcore

import (
	"fmt"
	"sync"
)

type mutableChunk struct {
	numPoints int64
	minT      int64
	maxT      int64
	metrics   sync.Map
	duration  int64
}

func newMutableChunk() chunk {
	return &mutableChunk{}
}

func (chunk *mutableChunk) insertRows(rows []Row) (outdatedRows []Row, err error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("no rows given")
	}
	return nil, nil
}

func (chunk *mutableChunk) clean() error {
	// 内存管理的部分在堆上，会被GC删除
	return nil
}

func (chunk *mutableChunk) selectDataPoints(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {

	return nil, nil
}

func (chunk *mutableChunk) minTimestamp() int64 {
	return chunk.minT
}

func (chunk *mutableChunk) maxTimestamp() int64 {
	return chunk.maxT
}

func (chunk *mutableChunk) count() int64 {
	return chunk.numPoints
}

func (chunk *mutableChunk) active() bool {
	return chunk.maxTimestamp()-chunk.minTimestamp()+1 < chunk.duration
}

func (chunk *mutableChunk) expired() bool {
	return false
}
