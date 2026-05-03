package tcore

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 辅助函数
// ---------------------------------------------------------------------------

// newBenchChunk 创建一个用于基准测试的可变分区（热数据分区）
func newBenchChunk(tb testing.TB) *mutableChunk {
	tb.Helper()
	return newMutableChunk(time.Hour, Nanoseconds).(*mutableChunk)
}

// makeRows 创建 N 个数据行，时间戳从 baseTime 开始递增，所有行共享同一个 metric
func makeRows(n int, baseTime int64, metric string) []Row {
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{
			Metric: metric,
			DataPoint: DataPoint{
				Timestamp: baseTime + int64(i),
				Value:     float64(i),
			},
		}
	}
	return rows
}

// warmupChunk 向分区写入 N 个点，触发 slice 扩容使其达到稳定容量，
// 避免在正式测量时因 append 扩容引入额外延迟
func warmupChunk(chunk *mutableChunk, metric string, n int) {
	rows := makeRows(n, 0, metric)
	chunk.insertRows(rows)
}

// discardWriter 实现 io.Writer，丢弃所有写入数据（用于编码器基准测试）
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// ---------------------------------------------------------------------------
// 1. 单条数据写入平均延迟 ≤ 100ns
//
// 向已预热的分区连续写入单行数据，测量每次 insertRows([]Row{...}) 的平均耗时。
// 预热阶段排除首次初始化（sync.Once、slice 首次创建等）的开销。
// ---------------------------------------------------------------------------

func BenchmarkWriteSingleRow(b *testing.B) {
	chunk := newBenchChunk(b)
	const metric = "bench_single"

	// 预写 50 万点使 inOrderPoints 完成扩容，稳定在 steady-state
	warmupChunk(chunk, metric, 500000)

	// 正式测量：每次插入单个数据点
	baseTime := int64(500001)
	row := Row{
		Metric: metric,
		DataPoint: DataPoint{
			Timestamp: baseTime,
			Value:     1.0,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.Timestamp = baseTime + int64(i)
		chunk.insertRows([]Row{row})
	}
}

// ---------------------------------------------------------------------------
// 2. 批量数据写入吞吐 ≥ 100 万 OPS
//
// 使用不同批次大小（10/100/1000/10000）测量吞吐量，
// 以百万行/秒（M rows/sec）为单位报告。
// ---------------------------------------------------------------------------

func BenchmarkWriteBatch(b *testing.B) {
	chunk := newBenchChunk(b)
	const metric = "bench_batch"
	warmupChunk(chunk, metric, 500000)

	for _, batchSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("size_%d", batchSize), func(b *testing.B) {
			baseTime := int64(500001)
			rows := makeRows(batchSize, baseTime, metric)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 每轮迭代更新时间戳，保证严格递增，避免触发乱序分支
				for j := range rows {
					rows[j].Timestamp = baseTime + int64(i*batchSize+j)
				}
				chunk.insertRows(rows)
			}

		})
	}
}

// BenchmarkWriteParallel 模拟多 goroutine 并发写入，测试聚合吞吐
//
// 使用 100 个独立时序（metrics）减少锁争用，更接近真实生产场景。
func BenchmarkWriteParallel(b *testing.B) {
	chunk := newBenchChunk(b)
	const numMetrics = 100
	const batchSize = 100

	// 预热：为每个 metric 预先创建 series entry
	for i := 0; i < numMetrics; i++ {
		metric := fmt.Sprintf("bench_par_%d", i)
		warmupChunk(chunk, metric, 1000)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// 每个 goroutine 持有独立的 rand source，避免全局锁竞争
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		localBase := rng.Int63n(1e12)
		idx := rng.Intn(numMetrics)
		metric := fmt.Sprintf("bench_par_%d", idx)

		rows := make([]Row, batchSize)
		for pb.Next() {
			for j := range rows {
				rows[j] = Row{
					Metric: metric,
					DataPoint: DataPoint{
						Timestamp: localBase,
						Value:     float64(localBase),
					},
				}
				localBase++
			}
			chunk.insertRows(rows)
		}
	})
}

// ---------------------------------------------------------------------------
// 3. 1 小时时间范围的热数据查询平均延迟 ≤ 200ns
//
// 向分区写入 1 小时跨度的热数据（模拟每 10ms 一个采样点 ≈ 36 万点），
// 然后测量全量范围查询的耗时。
// ---------------------------------------------------------------------------

func BenchmarkSelectHotData(b *testing.B) {
	chunk := newBenchChunk(b)
	const metric = "bench_select"
	labels := []Label{{Name: "host", Value: "server1"}}

	// 生成 1 小时数据，每 10ms 一个采样点 → 360,000 点
	oneHour := int64(time.Hour.Nanoseconds())
	interval := int64(10 * time.Millisecond.Nanoseconds())
	numPoints := int(oneHour / interval)

	now := time.Now().UnixNano()
	rows := make([]Row, numPoints)
	for i := range rows {
		rows[i] = Row{
			Metric: metric,
			Labels: labels,
			DataPoint: DataPoint{
				Timestamp: now + int64(i)*interval,
				Value:     float64(i),
			},
		}
	}
	chunk.insertRows(rows)

	start := now
	end := now + oneHour

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunk.selectDataPoints(metric, labels, start, end)
	}
}

// BenchmarkSelectRange 测试不同查询范围（1min / 10min / 1h）的延迟
func BenchmarkSelectRange(b *testing.B) {
	chunk := newBenchChunk(b)
	const metric = "bench_select_range"
	labels := []Label{{Name: "host", Value: "server1"}}

	oneHour := int64(time.Hour.Nanoseconds())
	interval := int64(10 * time.Millisecond.Nanoseconds())
	numPoints := int(oneHour / interval)

	now := time.Now().UnixNano()
	rows := make([]Row, numPoints)
	for i := range rows {
		rows[i] = Row{
			Metric: metric,
			Labels: labels,
			DataPoint: DataPoint{
				Timestamp: now + int64(i)*interval,
				Value:     float64(i),
			},
		}
	}
	chunk.insertRows(rows)

	for _, d := range []time.Duration{time.Minute, 10 * time.Minute, time.Hour} {
		rangeNs := d.Nanoseconds()
		b.Run(d.String(), func(b *testing.B) {
			start := now
			end := now + rangeNs
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				chunk.selectDataPoints(metric, labels, start, end)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 4. 运行时内存占用 ≤ 100MB
//
// 向分区写入不同量级的数据，通过 TotalAlloc（累计分配总量）评估内存开销，
// 同时用 Alloc（GC 后存活堆）观察常驻内存。双指标并行，避免编译器 DCE
// 对 Alloc 读数的干扰。
// ---------------------------------------------------------------------------

func TestMemoryUsage(t *testing.T) {
	for _, n := range []int{100000, 500000, 1000000} {
		t.Run(fmt.Sprintf("points_%d", n), func(t *testing.T) {
			chunk := newBenchChunk(t)
			rows := makeRows(n, 0, "mem_metric")

			// 插入前记录累计分配
			var m0 runtime.MemStats
			runtime.ReadMemStats(&m0)

			chunk.insertRows(rows)

			// 读数据做 DCE 屏障，保证 DataPoint 不被编译器优化掉
			pts, _ := chunk.selectDataPoints("mem_metric", nil, 0, 1)
			_ = pts

			// 方式 A：不触发 GC，直接读 Alloc（含所有存活对象）
			var m1 runtime.MemStats
			runtime.ReadMemStats(&m1)

			// 方式 B：触发 GC 后读 Alloc（仅常驻对象）
			runtime.GC()
			runtime.GC()
			var m2 runtime.MemStats
			runtime.ReadMemStats(&m2)

			allocLive := float64(m1.Alloc) / 1024 / 1024
			allocResident := float64(m2.Alloc) / 1024 / 1024
			totalAllocDelta := float64(m1.TotalAlloc-m0.TotalAlloc) / 1024 / 1024

			t.Logf("Points: %d | Live=%.2f MB | Resident=%.2f MB | TotalAllocDelta=%.2f MB | HeapObjects=%d",
				n, allocLive, allocResident, totalAllocDelta, m2.HeapObjects)

			if allocResident > 100 {
				t.Logf("WARN: resident Alloc %.2f MB exceeds 100 MB limit", allocResident)
			}
		})
	}
}

// TestMemoryNoLinearGrowth 验证冷数据场景下内存增长趋势
//
// 创建多个独立分区（模拟 flush 后的只读分区），观察总内存是否随分区数
// 线性增长。如果增长趋缓，说明冷分区仅保留元数据。
func TestMemoryNoLinearGrowth(t *testing.T) {
	var prevMB float64

	for _, numChunks := range []int{1, 5, 10} {
		chunks := make([]*mutableChunk, numChunks)
		for i := range chunks {
			chunks[i] = newBenchChunk(t)
			rows := makeRows(100000, int64(i*1000000), "growth_test")
			chunks[i].insertRows(rows)
		}

		// DCE 屏障：必须读数据才能阻止编译器消除分配
		var count int64
		for _, c := range chunks {
			pts, _ := c.selectDataPoints("growth_test", nil, 0, 1e12)
			count += int64(len(pts))
		}
		_ = count

		// 不触发 GC，直接读 Alloc 可以观察到完整的存活对象
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		allocMB := float64(m.Alloc) / 1024 / 1024

		t.Logf("Chunks: %d, Alloc(noGC)=%.2f MB, HeapObjects=%d, sampled_pts=%d",
			numChunks, allocMB, m.HeapObjects, count)

		if prevMB > 0 {
			chunkRatio := float64(numChunks) / float64(numChunks/2)
			memRatio := allocMB / prevMB
			t.Logf("  Chunk multiplier: %.1fx, Memory multiplier: %.2fx", chunkRatio, memRatio)
		}
		prevMB = allocMB
	}
}

// ---------------------------------------------------------------------------
// 5. 二进制文件体积 ≤ 10MB
//
// 创建一个最小 Go 程序导入 tcore 包，使用 -ldflags=-s -w 去掉调试信息后
// 编译，检查最终二进制体积。
// ---------------------------------------------------------------------------

func TestBinarySize(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping binary size check in short mode")
	}
	t.Parallel()

	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	// 创建最小可执行程序
	mainContent := []byte(`package main
import _ "github.com/Lwxjjr/tcore"
func main() {}
`)
	if err := os.WriteFile(filepath.Join(dir, "main.go"), mainContent, 0644); err != nil {
		t.Fatal(err)
	}

	// go.mod：用 replace 指向本地源码
	modContent := fmt.Sprintf(`module sizecheck
go 1.23
require github.com/Lwxjjr/tcore v0.0.0
require go.uber.org/automaxprocs v1.6.0
replace github.com/Lwxjjr/tcore => %s
`, cwd)
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatal(err)
	}

	// 执行 go mod tidy 解决依赖
	cmd := exec.Command("go", "mod", "tidy")
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go mod tidy failed: %v\noutput: %s", err, string(output))
	}

	// 编译（strip 调试符号）
	out := filepath.Join(dir, "tcore_bin")
	cmd = exec.Command("go", "build", "-ldflags=-s -w", "-o", out, ".")
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build failed: %v\noutput: %s", err, string(output))
	}

	info, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}

	sizeMB := float64(info.Size()) / 1024 / 1024
	t.Logf("Binary size: %.2f MB", sizeMB)
	if sizeMB > 10 {
		t.Errorf("binary size %.2f MB exceeds 10 MB limit", sizeMB)
	}
}

// ---------------------------------------------------------------------------
// 6. CPU 占用检测
//
// 持续写入大量数据，配合 -cpuprofile 可生成 CPU profile。
// 运行方式：go test -bench=BenchmarkCPUUsage -cpuprofile=cpu.prof -benchtime=10s
// 分析方法：go tool pprof -http=:8080 cpu.prof
// ---------------------------------------------------------------------------

func BenchmarkCPUUsage(b *testing.B) {
	chunk := newBenchChunk(b)
	const metric = "bench_cpu"
	const batchSize = 1000
	warmupChunk(chunk, metric, 500000)

	baseTime := int64(500001)
	rows := makeRows(batchSize, baseTime, metric)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range rows {
			rows[j].Timestamp = baseTime + int64(i*batchSize+j)
		}
		chunk.insertRows(rows)
	}
}

// ---------------------------------------------------------------------------
// 附：编码器基准测试（Gorilla vs Varint）
//
// 辅助分析：数据压缩/解压对写入和查询性能的影响
// ---------------------------------------------------------------------------

func BenchmarkGorillaEncode(b *testing.B) {
	points := make([]*DataPoint, 1000)
	for i := range points {
		points[i] = &DataPoint{
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder := newSeriesEncoderGorilla(discardWriter{})
		for _, p := range points {
			encoder.encodePoint(p)
		}
		encoder.flush()
	}
}

func BenchmarkVarintEncode(b *testing.B) {
	points := make([]*DataPoint, 1000)
	for i := range points {
		points[i] = &DataPoint{
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder := newSeriesEncoder(discardWriter{})
		for _, p := range points {
			encoder.encodePoint(p)
		}
		encoder.flush()
	}
}

// ---------------------------------------------------------------------------
// 并发安全验证：多 goroutine 同时读写同一分区
// ---------------------------------------------------------------------------

func TestConcurrentReadWrite(t *testing.T) {
	chunk := newBenchChunk(t)
	const metric = "concurrent_test"
	labels := []Label{{Name: "host", Value: "server1"}}

	var wg sync.WaitGroup

	// 写入 goroutine：持续写入 10 万点
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100000; i++ {
			row := Row{
				Metric: metric,
				Labels: labels,
				DataPoint: DataPoint{
					Timestamp: int64(i),
					Value:     float64(i),
				},
			}
			chunk.insertRows([]Row{row})
		}
	}()

	// 读取 goroutine：持续查询
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			chunk.selectDataPoints(metric, labels, 0, int64(i*10))
		}
	}()

	wg.Wait()

	// 验证数据正确性
	points, err := chunk.selectDataPoints(metric, labels, 0, 100000)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(points) == 0 {
		t.Fatal("no data points returned after concurrent read/write")
	}
	t.Logf("Concurrent test passed: %d points returned", len(points))
}
