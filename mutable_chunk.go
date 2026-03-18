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
	seriesMap sync.Map // 时间序列表
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
	var maxTimestamp int64
	var validRowsNum int64

	for _, row := range rows {
		// 如果时间戳早于当前最小时间戳，返回过期数据
		if row.Timestamp < chunk.minTimestamp() {
			outdatedRows = append(outdatedRows, row)
			continue
		}

		// 追踪最大时间戳（非原子操作）
		if row.Timestamp > maxTimestamp {
			maxTimestamp = row.Timestamp
		}

		// 生成唯一标识符
		key := marshalKey(row.Metric, row.Labels)

		// 获取或创建该 metricKey 的数据点列表
		series := chunk.getSeries(key)

		// 插入点位
		count := atomic.LoadInt64(&series.count)
		series.mu.Lock()
		defer series.mu.Unlock()
		// 顺序插入
		if series.inOrderPoints[count-1].Timestamp < row.DataPoint.Timestamp {
			series.inOrderPoints = append(series.inOrderPoints, &row.DataPoint)
			atomic.StoreInt64(&series.maxT, row.DataPoint.Timestamp)
			atomic.AddInt64(&series.count, 1)
			return
		}
		// 乱序超出
		series.outOfOrderPoints = append(series.outOfOrderPoints, &row.DataPoint)
		validRowsNum++
	}

	// 统一更新最大时间戳和计数器（减少原子操作）
	if validRowsNum > 0 {
		// 使用 CAS 循环更新 maxT，保证并发安全
		for {
			oldMax := atomic.LoadInt64(&chunk.maxT)
			if maxTimestamp <= oldMax {
				break
			}
			if atomic.CompareAndSwapInt64(&chunk.maxT, oldMax, maxTimestamp) {
				break
			}
		}
		atomic.AddInt64(&chunk.numPoints, validRowsNum)
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

func (chunk *mutableChunk) getSeries(key string) *series {
	if value, ok := chunk.seriesMap.Load(key); ok {
		return value.(*series)
	}
	newSeries := &series{
		inOrderPoints:    make([]*DataPoint, 0, 1000),
		outOfOrderPoints: make([]*DataPoint, 0),
	}
	value, _ := chunk.seriesMap.LoadOrStore(key, newSeries)
	return value.(*series)
}

// series 代表内存中一条独立的时间序列
// 只负责单一时间轴的数据点管理，不包含任何路由逻辑
type series struct {
	mu               *sync.RWMutex
	key              string
	inOrderPoints    []*DataPoint // 有序点位
	outOfOrderPoints []*DataPoint // 乱序点位
	minT             int64
	maxT             int64
	count            int64
}
