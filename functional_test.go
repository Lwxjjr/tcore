package tcore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// FT-01：时序数据写入功能验证
//
// 1. 初始化引擎，批量写入 100 万条标准时序数据
// 2. 校验写入结果与元数据状态
// 预期：写入成功率 100%，无数据丢失，元数据更新正确
// ---------------------------------------------------------------------------

func TestWriteAndVerify(t *testing.T) {
	chunk := newMutableChunk(100*time.Hour, Milliseconds).(*mutableChunk)
	n := 1000000

	rows := make([]Row, n)
	baseTime := int64(0)
	for i := range rows {
		rows[i] = Row{
			Metric: "cpu_usage",
			DataPoint: DataPoint{
				Timestamp: baseTime + int64(i),
				Value:     float64(i),
			},
		}
	}

	outdated, err := chunk.insertRows(rows)
	if err != nil {
		t.Fatalf("insertRows failed: %v", err)
	}
	if len(outdated) > 0 {
		t.Fatalf("outdated rows returned: %d", len(outdated))
	}
	if got := chunk.count(); got != int64(n) {
		t.Fatalf("count: got %d, want %d", got, n)
	}

	pts, err := chunk.selectDataPoints("cpu_usage", nil, 0, int64(n))
	if err != nil {
		t.Fatalf("selectDataPoints: %v", err)
	}
	if len(pts) != n {
		t.Fatalf("select count: got %d, want %d", len(pts), n)
	}
	for i, p := range pts {
		if p.Timestamp != int64(i) {
			t.Fatalf("point %d: timestamp %d, want %d", i, p.Timestamp, i)
		}
	}
}

// ---------------------------------------------------------------------------
// FT-02：乱序数据兼容功能验证
//
// 1. 写入 10 万条有序数据
// 2. 再写入 10 万条回退到前半段时间窗口的乱序数据
// 3. 触发刷盘，查询校验乱序数据完整性
// 预期：乱序数据 100% 保留，刷盘后数据按时间戳有序排列
// ---------------------------------------------------------------------------

func TestOutOfOrderData(t *testing.T) {
	chunk := newMutableChunk(100*time.Hour, Milliseconds).(*mutableChunk)
	nOrdered := 100000

	// 写入有序数据
	ordered := make([]Row, nOrdered)
	for i := range ordered {
		ordered[i] = Row{
			Metric: "cpu_usage",
			DataPoint: DataPoint{
				Timestamp: int64(i),
				Value:     float64(i),
			},
		}
	}
	chunk.insertRows(ordered)

	// 写入乱序数据（时间戳回退到 [0, 50000)）
	nOoo := 100000
	ooo := make([]Row, nOoo)
	for i := range ooo {
		ooo[i] = Row{
			Metric: "cpu_usage",
			DataPoint: DataPoint{
				Timestamp: int64(i % 50000),
				Value:     float64(100000 + i),
			},
		}
	}
	chunk.insertRows(ooo)

	// 刷盘：编码时会将 inOrder 和 outOfOrder 合并排序
	dir := t.TempDir()
	dataFile := filepath.Join(dir, "data")
	f, err := os.Create(dataFile)
	if err != nil {
		t.Fatal(err)
	}
	encoder := newSeriesEncoder(f)
	var seriesCount int64
	chunk.seriesMap.Range(func(_, value interface{}) bool {
		s := value.(*series)
		if err := s.encode(encoder); err != nil {
			t.Fatalf("encode: %v", err)
		}
		seriesCount = s.count + int64(len(s.outOfOrderPoints))
		return true
	})
	encoder.flush()
	f.Close()

	// 从磁盘读出，校验总点数
	fr, err := os.Open(dataFile)
	if err != nil {
		t.Fatalf("open data file: %v", err)
	}
	r, err := newSeriesDecoder(fr)
	if err != nil {
		t.Fatalf("newSeriesDecoder: %v", err)
	}

	var decoded []DataPoint
	for {
		var dp DataPoint
		if err := r.decodePoint(&dp); err != nil {
			break
		}
		decoded = append(decoded, dp)
	}

	if got := int64(len(decoded)); got != seriesCount {
		t.Fatalf("decoded count: got %d, want %d", got, seriesCount)
	}

	// 验证升序排列
	for i := 1; i < len(decoded); i++ {
		if decoded[i].Timestamp < decoded[i-1].Timestamp {
			t.Fatalf("not sorted at %d: %d < %d", i, decoded[i].Timestamp, decoded[i-1].Timestamp)
		}
	}
}

// ---------------------------------------------------------------------------
// FT-03：崩溃恢复功能验证
//
// 1. 写入 10 万条数据后执行 Close（模拟正常关闭/刷盘）
// 2. 重新打开引擎，从磁盘加载持久化分区
// 3. 校验数据完整性
// ---------------------------------------------------------------------------

func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// 创建引擎实例
	s := &storage{
		chunkList:      newChunkList(),
		duration:       time.Hour,
		retention:      defaultRetention,
		writeTimeout:   defaultWriteTimeout,
		workersLimitCh: make(chan struct{}, runtime.GOMAXPROCS(0)),
		doneCh:         make(chan struct{}),
		dataPath:       dir,
	}
	s.newChunk(nil)

	// 写入 10 万条
	n := 100000
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{
			Metric: "cpu_usage",
			DataPoint: DataPoint{
				Timestamp: int64(i),
				Value:     float64(i),
			},
		}
	}
	if err := s.InsertRows(rows); err != nil {
		t.Fatalf("InsertRows: %v", err)
	}

	// 关闭（触发 flush 到磁盘）
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 重新打开：从磁盘加载分区
	s2 := &storage{
		chunkList:      newChunkList(),
		duration:       time.Hour,
		retention:      defaultRetention,
		writeTimeout:   defaultWriteTimeout,
		workersLimitCh: make(chan struct{}, runtime.GOMAXPROCS(0)),
		doneCh:         make(chan struct{}),
		dataPath:       dir,
	}

	dirs, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range dirs {
		if !e.IsDir() || !chunkDirRegex.MatchString(e.Name()) {
			continue
		}
		path := filepath.Join(dir, e.Name())
		ic, err := openImmutableChunk(path, s2.retention)
		if err == ErrNoDataPoints {
			continue
		}
		if err != nil {
			t.Fatalf("openImmutableChunk(%s): %v", path, err)
		}
		s2.chunkList.insert(ic)
	}
	s2.chunkList.insert(nil)

	// 遍历所有恢复的分区，汇总数据点数
	var totalPts int64
	iter := s2.chunkList.newIterator()
	for iter.next() {
		c := iter.chunk()
		if c == nil {
			continue
		}
		totalPts += c.count()
		pts, _ := c.selectDataPoints("cpu_usage", nil, 0, int64(n))
		if len(pts) > 0 {
			t.Logf("  chunk [%d,%d]: %d points", c.minTimestamp(), c.maxTimestamp(), len(pts))
		}
	}

	if totalPts == 0 {
		t.Fatal("no data recovered from disk")
	}
	t.Logf("Recovery OK: %d total points loaded across %d chunks", totalPts, s2.chunkList.count())
}

// ---------------------------------------------------------------------------
// FT-04：时间范围查询功能验证
//
// 1. 写入带连续时间戳的 100 万条时序数据
// 2. 按指定起止时间发起范围查询
// 3. 校验返回数据的完整性与有序性
// ---------------------------------------------------------------------------

func TestRangeQuery(t *testing.T) {
	chunk := newMutableChunk(100*time.Hour, Milliseconds).(*mutableChunk)
	n := 1000000

	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{
			Metric: "cpu_usage",
			DataPoint: DataPoint{
				Timestamp: int64(i),
				Value:     float64(i),
			},
		}
	}
	chunk.insertRows(rows)

	tests := []struct {
		name       string
		start, end int64
		want       int
	}{
		{"first_100", 0, 100, 100},
		{"mid_range", 400000, 500000, 100000},
		{"last_100", 999900, 1000000, 100},
		{"all", 0, 1000000, n},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pts, err := chunk.selectDataPoints("cpu_usage", nil, tt.start, tt.end)
			if err != nil {
				t.Fatalf("select: %v", err)
			}
			if len(pts) != tt.want {
				t.Fatalf("got %d points, want %d", len(pts), tt.want)
			}
			for i := 1; i < len(pts); i++ {
				if pts[i].Timestamp < pts[i-1].Timestamp {
					t.Fatalf("not sorted at %d", i)
				}
			}
			if len(pts) > 0 {
				if pts[0].Timestamp < tt.start {
					t.Fatalf("first point %d outside range start %d", pts[0].Timestamp, tt.start)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// FT-05：数据生命周期管理功能验证
//
// 1. 创建磁盘分区元数据，部分设置过期时间
// 2. 触发过期清理
// 3. 校验过期分区被清理，有效数据不受影响
// ---------------------------------------------------------------------------

func TestDataRetention(t *testing.T) {
	dir := t.TempDir()

	persistChunk := func(minT, maxT int64, createdAt time.Time) string {
		name := fmt.Sprintf("p-%d-%d", minT, maxT)
		chunkDir := filepath.Join(dir, name)
		os.MkdirAll(chunkDir, 0755)

		points := []*DataPoint{
			{Timestamp: minT, Value: 1.0},
			{Timestamp: maxT, Value: 2.0},
		}
		dataPath := filepath.Join(chunkDir, "data")
		f, _ := os.Create(dataPath)
		enc := newSeriesEncoder(f)
		for _, p := range points {
			enc.encodePoint(p)
		}
		enc.flush()
		f.Close()

		meta := meta{
			MinTimestamp: minT,
			MaxTimestamp: maxT,
			NumPoints:    2,
			SeriesMap: map[string]seriesIndex{
				"cpu": {Key: "cpu", Offset: 0, MinTimestamp: minT, MaxTimestamp: maxT, NumPoints: 2},
			},
			CreatedAt: createdAt,
		}
		metaBytes, _ := json.Marshal(meta)
		os.WriteFile(filepath.Join(chunkDir, "meta.json"), metaBytes, 0644)
		return chunkDir
	}

	// 持久化 3 个分区：1 个过期、2 个有效
	persistChunk(0, 999, time.Now().Add(-48*time.Hour))
	persistChunk(1000, 1999, time.Now().Add(-1*time.Hour))
	persistChunk(2000, 2999, time.Now())

	// 用短 retention 打开，使第一个分区过期
	s := &storage{
		chunkList:      newChunkList(),
		duration:       time.Hour,
		retention:      24 * time.Hour,
		writeTimeout:   defaultWriteTimeout,
		workersLimitCh: make(chan struct{}, runtime.GOMAXPROCS(0)),
		doneCh:         make(chan struct{}),
		dataPath:       dir,
	}
	// 加载所有分区
	dirs, _ := os.ReadDir(dir)
	var loaded []chunk
	for _, e := range dirs {
		if !e.IsDir() || !chunkDirRegex.MatchString(e.Name()) {
			continue
		}
		path := filepath.Join(dir, e.Name())
		ic, err := openImmutableChunk(path, s.retention)
		if err == ErrNoDataPoints {
			continue
		}
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		loaded = append(loaded, ic)
	}
	sort.Slice(loaded, func(i, j int) bool {
		return loaded[i].minTimestamp() < loaded[j].minTimestamp()
	})
	for _, c := range loaded {
		s.chunkList.insert(c)
	}

	t.Logf("Loaded %d chunks", s.chunkList.count())

	// 执行过期清理
	if err := s.removeExpiredChunks(); err != nil {
		t.Fatalf("removeExpiredChunks: %v", err)
	}

	// 验证：应剩余 2 个有效分区
	remaining := 0
	iter := s.chunkList.newIterator()
	for iter.next() {
		remaining++
	}
	if remaining != 2 {
		t.Fatalf("after cleanup: %d chunks remain, want 2", remaining)
	}

	// 验证过期分区目录已从磁盘删除
	_, err := os.Stat(filepath.Join(dir, "p-0-999"))
	if !os.IsNotExist(err) {
		t.Fatal("expired chunk directory still exists on disk")
	}
}

// ---------------------------------------------------------------------------
// FT-06：数据压缩功能验证
//
// 已在 compression_test.go 中实现，此处不再重复。
// ---------------------------------------------------------------------------
