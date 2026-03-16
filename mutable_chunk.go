package tcore

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type mutableChunk struct {
	numPoints int64
	minT      int64
	maxT      int64
	metrics   sync.Map
	duration  int64
	once      sync.Once
}

func newMutableChunk() chunk {
	return &mutableChunk{}
}

func (chunk *mutableChunk) insertRows(rows []Row) (outdatedRows []Row, err error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("no rows given")
	}

	// 使用 sync.Once 保证初始化只执行一次
	chunk.once.Do(func() {
		min := rows[0].Timestamp
		for i := range rows {
			row := rows[i]
			if row.Timestamp < min {
				min = row.Timestamp
			}
		}
		atomic.StoreInt64(&chunk.minT, min)
	})

	// 遍历所有行，过滤过期数据并插入有效数据
	for _, row := range rows {
		// 如果时间戳早于当前最小时间戳，返回过期数据
		if row.Timestamp < atomic.LoadInt64(&chunk.minT) {
			outdatedRows = append(outdatedRows, row)
			continue
		}

		// 更新最大时间戳
		currentMax := atomic.LoadInt64(&chunk.maxT)
		if row.Timestamp > currentMax {
			atomic.StoreInt64(&chunk.maxT, row.Timestamp)
		}

		// 生成唯一标识符
		metricKey := marshalMetricName(row.Metric, row.Labels)

		// 获取或创建该 metric 的数据点列表
		value, _ := chunk.metrics.LoadOrStore(metricKey, &[]*DataPoint{})
		dataPoints := value.(*[]*DataPoint)

		// 插入数据点
		newDataPoint := &DataPoint{
			Value:     row.Value,
			Timestamp: row.Timestamp,
		}
		*dataPoints = append(*dataPoints, newDataPoint)

		// 更新计数器
		atomic.AddInt64(&chunk.numPoints, 1)
	}

	return outdatedRows, nil
}

func (chunk *mutableChunk) clean() error {
	// 内存管理的部分在堆上，会被GC删除
	return nil
}

func (chunk *mutableChunk) selectDataPoints(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {

	return nil, nil
}

func (chunk *mutableChunk) minTimestamp() int64 {
	return atomic.LoadInt64(&chunk.minT)
}

func (chunk *mutableChunk) maxTimestamp() int64 {
	return atomic.LoadInt64(&chunk.maxT)
}

func (chunk *mutableChunk) count() int64 {
	return atomic.LoadInt64(&chunk.numPoints)
}

func (chunk *mutableChunk) active() bool {
	return chunk.maxTimestamp()-chunk.minTimestamp()+1 < chunk.duration
}

func (chunk *mutableChunk) expired() bool {
	return false
}
