package tcore

import (
	"sync"
	"time"
)

// 阈值配置
const (
	BlockMaxPoints     = 1000             // 触发刷盘的数量阈值
	ForceFlushInterval = 60 * time.Second // 触发强制刷盘的时间阈值
)

// Series 代表一个传感器的专属时间线
type Series struct {
	ID            uint32
	mu            sync.RWMutex // 读写锁：保护下方所有字段
	activeBuffer  []Point      // 热数据：待落盘的点
	blocks        []*BlockMeta // 冷索引：已落盘的数据块目录
	lastFlushTime time.Time    // 计时器：上次成功刷盘的时间
}

func newSeries(id uint32) *Series {
	return &Series{
		ID:            id,
		activeBuffer:  make([]Point, 0, BlockMaxPoints), // 预分配容量，避免扩容开销
		blocks:        make([]*BlockMeta, 0),
		lastFlushTime: time.Now(),
	}
}

// ==========================================
// ✍️ 写入路径 (Write Path)
// ==========================================

// append 追加数据。如果达到阈值，会"窃取"并返回数据供调用方落盘。
func (s *Series) append(point Point) []Point {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.activeBuffer = append(s.activeBuffer, point)

	// ⚡️ 触发条件 A：数量满了
	if len(s.activeBuffer) >= BlockMaxPoints {
		return s.stealLocked()
	}
	return nil // 没满，返回 nil，外部无需执行写盘
}

// checkForTicker 供后台 Ticker 调用，检查是否因为超时需要强制刷盘
func (s *Series) checkForTicker() []Point {
	s.mu.Lock()
	defer s.mu.Unlock()

	// ⏰ 触发条件 B：有数据，且距离上次刷盘超过了设定的最大间隔
	if len(s.activeBuffer) > 0 && time.Since(s.lastFlushTime) >= ForceFlushInterval {
		return s.stealLocked()
	}
	return nil
}

// stealLocked 是核心的“偷梁换柱”魔法（调用方必须持有写锁）
// 它将底层数组彻底剥离，换上新的，保证写磁盘时不会阻塞新的 Append
func (s *Series) stealLocked() []Point {
	dataToSteal := s.activeBuffer

	// 分配全新的底层数组
	s.activeBuffer = make([]Point, 0, BlockMaxPoints)
	s.lastFlushTime = time.Now() // 重置计时器

	return dataToSteal
}

// addBlockMeta 数据成功落盘后，由外部调用此方法将元数据登记造册
func (s *Series) addBlockMeta(meta *BlockMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocks = append(s.blocks, meta)
}

// ==========================================
// 🔍 查询路径 (Query Path)
// ==========================================

// getHotData 获取尚未落盘的热数据（安全拷贝）
func (s *Series) getHotData() []Point {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 必须做深度拷贝，防止外部读取时切片被 stealLocked 替换或修改
	result := make([]Point, len(s.activeBuffer))
	copy(result, s.activeBuffer)
	return result
}

// findBlocks 查询冷数据索引：找出在指定时间范围内的所有 Block
func (s *Series) findBlocks(start, end int64) []*BlockMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*BlockMeta
	for _, meta := range s.blocks {
		// 时间范围过滤
		if meta.MaxTime < start || meta.MinTime > end {
			continue
		}
		// 这里存储的是指针，外部拿到指针后去调用 Manager 读盘
		result = append(result, meta)
	}
	return result
}
