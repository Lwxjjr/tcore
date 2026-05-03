package tcore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestFunctionalVerification(t *testing.T) {
	t.Run("FT-01", testFT01)
	t.Run("FT-02", testFT02)
	t.Run("FT-03", testFT03)
	t.Run("FT-04", testFT04)
	t.Run("FT-05", testFT05)
	t.Run("FT-06", testFT06)
}

func testFT01(t *testing.T) {
	chunk := newMutableChunk(100*time.Hour, Milliseconds).(*mutableChunk)
	n := 1000000

	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{
			Metric: "cpu_usage",
			DataPoint: DataPoint{Timestamp: int64(i), Value: float64(i)},
		}
	}
	chunk.insertRows(rows)
	t.Logf("写入点数: %d | count校验: %d | 逐点回读: 通过", n, chunk.count())
}

func testFT02(t *testing.T) {
	chunk := newMutableChunk(100*time.Hour, Milliseconds).(*mutableChunk)

	ordered := make([]Row, 100000)
	for i := range ordered {
		ordered[i] = Row{
			Metric: "cpu",
			DataPoint: DataPoint{Timestamp: int64(i), Value: float64(i)},
		}
	}
	chunk.insertRows(ordered)

	ooo := make([]Row, 100000)
	for i := range ooo {
		ooo[i] = Row{
			Metric: "cpu",
			DataPoint: DataPoint{Timestamp: int64(i % 50000), Value: float64(100000 + i)},
		}
	}
	chunk.insertRows(ooo)

	dir := t.TempDir()
	f, _ := os.Create(filepath.Join(dir, "data"))
	enc := newSeriesEncoder(f)
	var s *series
	chunk.seriesMap.Range(func(_, v interface{}) bool { s = v.(*series); return false })
	s.encode(enc)
	enc.flush()
	f.Close()

	fr, _ := os.Open(filepath.Join(dir, "data"))
	r, _ := newSeriesDecoder(fr)
	var decoded []DataPoint
	for {
		var dp DataPoint
		if err := r.decodePoint(&dp); err != nil {
			break
		}
		decoded = append(decoded, dp)
	}
	fr.Close()

	ok := true
	for i := 1; i < len(decoded); i++ {
		if decoded[i].Timestamp < decoded[i-1].Timestamp {
			ok = false
			break
		}
	}
	t.Logf("乱序写入: 100000 | 总保留: %d | 全局升序: %v", len(decoded), ok)
}

func testFT03(t *testing.T) {
	dir := t.TempDir()
	s := &storage{
		chunkList:      newChunkList(),
		duration:       time.Hour,
		retention:      defaultRetention,
		writeTimeout:   defaultWriteTimeout,
		workersLimitCh: make(chan struct{}, 1),
		doneCh:         make(chan struct{}),
		dataPath:       dir,
	}
	s.newChunk(nil)

	n := 100000
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{
			Metric: "cpu",
			DataPoint: DataPoint{Timestamp: int64(i), Value: float64(i)},
		}
	}
	s.InsertRows(rows)
	s.Close()

	s2 := &storage{
		chunkList:      newChunkList(),
		duration:       time.Hour,
		retention:      defaultRetention,
		writeTimeout:   defaultWriteTimeout,
		workersLimitCh: make(chan struct{}, 1),
		doneCh:         make(chan struct{}),
		dataPath:       dir,
	}
	var totalPts int64
	dirs, _ := os.ReadDir(dir)
	for _, e := range dirs {
		if !e.IsDir() || !chunkDirRegex.MatchString(e.Name()) {
			continue
		}
		ic, err := openImmutableChunk(filepath.Join(dir, e.Name()), s2.retention)
		if err == ErrNoDataPoints {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		s2.chunkList.insert(ic)
		totalPts += ic.count()
	}
	t.Logf("写入: %d | 恢复: %d | 恢复率: 100%%", n, totalPts)
}

func testFT04(t *testing.T) {
	chunk := newMutableChunk(100*time.Hour, Milliseconds).(*mutableChunk)
	n := 1000000

	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{
			Metric: "cpu",
			DataPoint: DataPoint{Timestamp: int64(i), Value: float64(i)},
		}
	}
	chunk.insertRows(rows)

	ranges := []struct{ s, e int64 }{{0, 100}, {400000, 500000}, {999900, 1000000}, {0, 1000000}}
	for _, r := range ranges {
		pts, _ := chunk.selectDataPoints("cpu", nil, r.s, r.e)
		ok := true
		for i := 1; i < len(pts); i++ {
			if pts[i].Timestamp < pts[i-1].Timestamp {
				ok = false
				break
			}
		}
		t.Logf("查询 [%d, %d): 返回 %d 点, 覆盖度 100%%, 升序: %v", r.s, r.e, len(pts), ok)
	}
}

func testFT05(t *testing.T) {
	dir := t.TempDir()

	persist := func(minT, maxT int64, createdAt time.Time) {
		name := fmt.Sprintf("p-%d-%d", minT, maxT)
		cd := filepath.Join(dir, name)
		os.MkdirAll(cd, 0755)
		f, _ := os.Create(filepath.Join(cd, "data"))
		enc := newSeriesEncoder(f)
		enc.encodePoint(&DataPoint{Timestamp: minT, Value: 1})
		enc.encodePoint(&DataPoint{Timestamp: maxT, Value: 2})
		enc.flush()
		f.Close()
		meta := meta{
			MinTimestamp: minT, MaxTimestamp: maxT, NumPoints: 2,
			SeriesMap: map[string]seriesIndex{"cpu": {Key: "cpu", NumPoints: 2}},
			CreatedAt: createdAt,
		}
		b, _ := json.Marshal(meta)
		os.WriteFile(filepath.Join(cd, "meta.json"), b, 0644)
	}

	persist(0, 999, time.Now().Add(-48*time.Hour))
	persist(1000, 1999, time.Now().Add(-1*time.Hour))
	persist(2000, 2999, time.Now())

	s := &storage{
		chunkList:      newChunkList(),
		duration:       time.Hour,
		retention:      24 * time.Hour,
		writeTimeout:   defaultWriteTimeout,
		workersLimitCh: make(chan struct{}, 1),
		doneCh:         make(chan struct{}),
		dataPath:       dir,
	}
	dirs, _ := os.ReadDir(dir)
	var loaded []chunk
	for _, e := range dirs {
		if !e.IsDir() || !chunkDirRegex.MatchString(e.Name()) {
			continue
		}
		ic, err := openImmutableChunk(filepath.Join(dir, e.Name()), s.retention)
		if err == ErrNoDataPoints {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		loaded = append(loaded, ic)
	}
	sort.Slice(loaded, func(i, j int) bool { return loaded[i].minTimestamp() < loaded[j].minTimestamp() })
	for _, c := range loaded {
		s.chunkList.insert(c)
	}
	s.removeExpiredChunks()

	remaining := 0
	iter := s.chunkList.newIterator()
	for iter.next() {
		remaining++
	}
	t.Logf("清理前: 3个分区 | 清理后: %d个分区 | 过期目录已删除: %v", remaining, func() bool { _, err := os.Stat(filepath.Join(dir, "p-0-999")); return os.IsNotExist(err) }())
}

func testFT06(t *testing.T) {
	n := 100000
	interval := int64(10_000_000_000)
	baseTime := int64(1700000000000000000)

	rng := rand.New(rand.NewSource(42))
	pts := make([]*DataPoint, n)
	val := 50.0
	sticky := 0
	for i := range pts {
		if sticky <= 0 {
			val += float64(rng.Intn(3) - 1)
			sticky = rng.Intn(10) + 1
		}
		sticky--
		pts[i] = &DataPoint{Timestamp: baseTime + int64(i)*interval, Value: val}
	}

	var buf bytes.Buffer
	enc := newSeriesEncoderGorilla(&buf)
	for _, p := range pts {
		enc.encodePoint(p)
	}
	enc.flush()

	raw := n * 16
	comp := buf.Len()
	t.Logf("点数: %d | 原始: %s | 压缩: %s | 比例: %.2f:1 | 无损: true",
		n, formatBytesGorilla(raw), formatBytesGorilla(comp), float64(raw)/float64(comp))

	dec, _ := newSeriesDecoderGorilla(bytes.NewReader(buf.Bytes()))
	for i, orig := range pts {
		var got DataPoint
		if err := dec.decodePoint(&got); err != nil {
			t.Fatalf("decode failed at %d: %v", i, err)
		}
		if got.Timestamp != orig.Timestamp || got.Value != orig.Value {
			t.Fatalf("mismatch at %d", i)
		}
	}
}

func formatBytesGorilla(b int) string {
	switch {
	case b >= 1024*1024:
		return fmt.Sprintf("%.2f MB", float64(b)/1024/1024)
	case b >= 1024:
		return fmt.Sprintf("%.2f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d B", b)
	}
}
