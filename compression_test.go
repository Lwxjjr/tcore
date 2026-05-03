package tcore

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
)

// TestCompression 验证 Gorilla 编码器的压缩比和解压无失真。
//
// 生成 10 万点常规监控时序数据（10s 间隔），用 Gorilla 编码压缩后
// 计算压缩比，再解码回读逐点对比，确保无失真。
func TestCompression(t *testing.T) {
	n := 100000
	interval := int64(10_000_000_000) // 10 秒
	baseTime := int64(1700000000000000000)

	// 生成模拟监控数据：整型值，连续多点保持不变（粘性随机游走）
	rng := rand.New(rand.NewSource(42))
	pts := make([]*DataPoint, n)
	val := 50.0
	sticky := 0
	for i := range pts {
		if sticky <= 0 {
			val += float64(rng.Intn(3) - 1) // -1, 0, or +1
			sticky = rng.Intn(10) + 1
		}
		sticky--
		pts[i] = &DataPoint{
			Timestamp: baseTime + int64(i)*interval,
			Value:     val,
		}
	}

	// Gorilla 编码
	var buf bytes.Buffer
	enc := newSeriesEncoderGorilla(&buf)
	for _, p := range pts {
		if err := enc.encodePoint(p); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	if err := enc.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// 压缩比
	rawBytes := n * 16
	compBytes := buf.Len()
	ratio := float64(rawBytes) / float64(compBytes)
	t.Logf("Points: %d, raw=%d bytes, compressed=%d bytes, ratio=%.2f:1",
		n, rawBytes, compBytes, ratio)

	if ratio < 12.3 {
		t.Errorf("compression ratio %.2f:1 < 12.3:1", ratio)
	}

	// 解码回读并逐点对比
	dec, err := newSeriesDecoderGorilla(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("newDecoder: %v", err)
	}
	for i, orig := range pts {
		var got DataPoint
		if err := dec.decodePoint(&got); err != nil {
			t.Fatalf("decode point %d: %v", i, err)
		}
		if got.Timestamp != orig.Timestamp {
			t.Fatalf("timestamp mismatch at %d: got %d, want %d", i, got.Timestamp, orig.Timestamp)
		}
		if math.Float64bits(got.Value) != math.Float64bits(orig.Value) {
			t.Fatalf("value mismatch at %d: got %v, want %v", i, got.Value, orig.Value)
		}
	}
	t.Logf("Roundtrip OK: all %d points verified lossless", n)
}
