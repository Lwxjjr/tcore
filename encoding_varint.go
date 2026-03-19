package tcore

import (
	"encoding/binary"
	"fmt"
	"io"
)

// seriesEncoder 定义了数据点编码的接口。
type seriesEncoder interface {
	encodePoint(point *DataPoint) error
	flush() error
}

// seriesDecoder 定义了从字节流中还原数据点的接口。
type seriesDecoder interface {
	decodePoint(dst *DataPoint) error
}

// newSeriesEncoder 创建一个使用 Varint + Delta 压缩的编码器。
func newSeriesEncoder(w io.Writer) seriesEncoder {
	return &varintEncoder{
		w: w,
	}
}

// varintEncoder 使用变长整数和增量进行简单高效的压缩。
type varintEncoder struct {
	w       io.Writer
	lastT   int64   // 上一个点的时间戳
	lastV   float64 // 上一个点的值（可选压缩）
	isFirst bool
}

func (e *varintEncoder) encodePoint(point *DataPoint) error {
	var tDelta int64
	if !e.isFirst {
		// 第一个点存储原始时间戳
		tDelta = point.Timestamp
		e.isFirst = true
	} else {
		// 后面存储与上一个点的差值（Delta）
		tDelta = point.Timestamp - e.lastT
	}

	// 1. 压缩存储时间戳 (Varint)
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, tDelta)
	if _, err := e.w.Write(buf[:n]); err != nil {
		return fmt.Errorf("failed to write timestamp delta: %w", err)
	}

	// 2. 存储数值 (目前直接存 8 字节原始 float64)
	// 如果未来想进一步压缩，可以对 Value 也做类似逻辑
	if err := binary.Write(e.w, binary.BigEndian, point.Value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}

	e.lastT = point.Timestamp
	return nil
}

func (e *varintEncoder) flush() error {
	// 简单实现不需要特殊的刷盘逻辑
	return nil
}

// newSeriesDecoder 创建一个从 Varint + Delta 格式读取的解码器。
func newSeriesDecoder(r io.Reader) (seriesDecoder, error) {
	// 确保 Reader 实现了 ByteReader 接口（binary.ReadVarint 的要求）
	br, ok := r.(io.ByteReader)
	if !ok {
		// 如果不是 ByteReader，则包装一层缓冲区
		return &varintDecoder{
			r: binaryReader{Reader: r},
		}, nil
	}
	return &varintDecoder{
		r: br,
	}, nil
}

type varintDecoder struct {
	r       io.ByteReader
	lastT   int64
	isFirst bool
}

func (d *varintDecoder) decodePoint(dst *DataPoint) error {
	// 1. 读取时间戳增量
	delta, err := binary.ReadVarint(d.r)
	if err != nil {
		return err // 可能是 io.EOF
	}

	// 还原原始时间戳
	if !d.isFirst {
		dst.Timestamp = delta
		d.isFirst = true
	} else {
		dst.Timestamp = d.lastT + delta
	}

	// 2. 读取 8 字节原始数值
	// 注意：binary.Read 需要 io.Reader 而不是 ByteReader
	// 我们需要一个能兼容两者的结构
	var val float64
	if br, ok := d.r.(binaryReader); ok {
		if err := binary.Read(br.Reader, binary.BigEndian, &val); err != nil {
			return fmt.Errorf("failed to read value: %w", err)
		}
	} else if reader, ok := d.r.(io.Reader); ok {
		if err := binary.Read(reader, binary.BigEndian, &val); err != nil {
			return fmt.Errorf("failed to read value: %w", err)
		}
	} else {
		return fmt.Errorf("unsupported reader type")
	}

	dst.Value = val
	d.lastT = dst.Timestamp
	return nil
}

// binaryReader 是一个简单的包装器，同时实现了 io.Reader 和 io.ByteReader。
type binaryReader struct {
	io.Reader
}

func (b binaryReader) ReadByte() (byte, error) {
	var buf [1]byte
	n, err := b.Read(buf[:])
	if n == 1 {
		return buf[0], nil
	}
	if err == nil {
		err = io.EOF
	}
	return 0, err
}
