package tcore

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type mutableChunk struct {
	numPoints int64
	minT      int64
	maxT      int64

	// 时间序列表
	seriesMap sync.Map
	once      sync.Once

	// duration 指定分区时间戳的范围
	duration int64
	// timestampPrecision 指定时间精度，支持纳秒、微秒、毫秒、秒
	timestampPrecision TimestampPrecision
}

func newMutableChunk(duration time.Duration, precision TimestampPrecision) chunk {
	var d int64
	switch precision {
	case Nanoseconds:
		d = duration.Microseconds()
	case Microseconds:
		d = duration.Microseconds()
	case Milliseconds:
		d = duration.Milliseconds()
	case Seconds:
		d = int64(duration.Seconds())
	default:
		d = duration.Nanoseconds()
	}
	return &mutableChunk{
		duration:           d,
		timestampPrecision: precision,
	}
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

		if row.Timestamp == 0 {
			row.Timestamp = toUnix(time.Now(), chunk.timestampPrecision)
		}

		// 追踪最大时间戳（非原子操作）
		if row.Timestamp > maxTimestamp {
			maxTimestamp = row.Timestamp
		}

		// 生成唯一标识符
		key := marshalKey(row.Metric, row.Labels)

		// 获取或创建该 metricKey 的数据点列表
		series := chunk.getSeries(key)
		series.insertPoint(&row.DataPoint)
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
	key := marshalKey(metric, labels)
	series := chunk.getSeries(key)
	return series.selectPoints(start, end), nil
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
	minTimeStamp     int64
	maxTimeStamp     int64
	count            int64
}

func (s *series) insertPoint(point *DataPoint) {
	// 插入点位
	count := atomic.LoadInt64(&s.count)
	s.mu.Lock()
	defer s.mu.Unlock()
	// 顺序插入
	if s.inOrderPoints[count-1].Timestamp < point.Timestamp {
		s.inOrderPoints = append(s.inOrderPoints, point)
		atomic.StoreInt64(&s.maxTimeStamp, point.Timestamp)
		atomic.AddInt64(&s.count, 1)
		return
	}
	// 乱序超出
	s.outOfOrderPoints = append(s.outOfOrderPoints, point)
}

func (s *series) selectPoints(start, end int64) []*DataPoint {
	count := atomic.LoadInt64(&s.count)
	minTimestamp := atomic.LoadInt64(&s.minTimeStamp)
	maxTimestamp := atomic.LoadInt64(&s.maxTimeStamp)
	if end <= minTimestamp || start >= maxTimestamp {
		return []*DataPoint{}
	}

	var startIdx, endIdx int
	s.mu.RLock()
	defer s.mu.RUnlock()
	// 通过 points 的有序性，二分查找获取开始索引
	startIdx = sort.Search(int(count), func(i int) bool {
		return s.inOrderPoints[i].Timestamp >= start
	})
	endIdx = sort.Search(int(count), func(i int) bool {
		return s.inOrderPoints[i].Timestamp >= end
	})
	return s.inOrderPoints[startIdx:endIdx]
}

func (s *series) encode(encoder seriesEncoder) error {
	// 排序乱序点
	sort.Slice(s.outOfOrderPoints, func(i, j int) bool {
		return s.outOfOrderPoints[i].Timestamp < s.outOfOrderPoints[j].Timestamp
	})

	// 将两个“子序列”合并成一个全局有序的“大序列”
	var outIndex, inIndex int
	for outIndex < len(s.outOfOrderPoints) && inIndex < len(s.inOrderPoints) {
		if s.outOfOrderPoints[outIndex].Timestamp < s.inOrderPoints[inIndex].Timestamp {
			if err := encoder.encodePoint(s.outOfOrderPoints[outIndex]); err != nil {
				return err
			}
			outIndex++
		} else {
			if err := encoder.encodePoint(s.inOrderPoints[inIndex]); err != nil {
				return err
			}
			inIndex++
		}
	}

	// 处理剩余点
	for outIndex < len(s.outOfOrderPoints) {
		if err := encoder.encodePoint(s.outOfOrderPoints[outIndex]); err != nil {
			return err
		}
		outIndex++
	}
	for inIndex < len(s.inOrderPoints) {
		if err := encoder.encodePoint(s.inOrderPoints[inIndex]); err != nil {
			return err
		}
		inIndex++
	}
	return nil
}

func toUnix(t time.Time, precision TimestampPrecision) int64 {
	switch precision {
	case Nanoseconds:
		return t.UnixNano()
	case Microseconds:
		return t.UnixNano() / 1e3
	case Milliseconds:
		return t.UnixNano() / 1e6
	case Seconds:
		return t.Unix()
	default:
		return t.UnixNano()
	}
}
