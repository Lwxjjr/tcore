package tcore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const (
	dataFileName = "data"
	metaFileName = "meta.json"
)

type immutableChunk struct {
	dirPath   string
	file      *os.File
	mmapData  []byte
	retention time.Duration // 数据的最大保持时长
	meta      meta
}

// meta是整个分区的元数据
type meta struct {
	MinTimestamp int64                  `json:"minTimestamp"`
	MaxTimestamp int64                  `json:"maxTimestamp"`
	NumPoints    int64                  `json:"numPoints"`
	SeriesMap    map[string]seriesIndex `json:"seriesMap"`
	CreatedAt    time.Time              `json:"createdAt"`
}

// seriesIndex 保存了如何从 mmap 切片中准确找到某一个具体指标数据的导航信息
type seriesIndex struct {
	Key          string `json:"key"`
	Offset       int64  `json:"offset"`
	MinTimestamp int64  `json:"minTimeStamp"`
	MaxTimestamp int64  `json:"maxTimeStamp"`
	NumPoints    int64  `json:"numPoints"`
}

// openImmutableChunk 将数据文件映射到内存中
func openImmutableChunk(dirPath string, retention time.Duration) (chunk, error) {
	if dirPath == "" {
		return nil, fmt.Errorf("dirPath is required")
	}
	metaFilePath := filepath.Join(dirPath, metaFileName)
	_, err := os.Stat(metaFilePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("invalid partition")
	}

	// 将数据映射到内存
	dataPath := filepath.Join(dirPath, dataFileName)
	f, err := os.Open(dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read data file: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch file info: %w", err)
	}
	if info.Size() == 0 {
		return nil, errors.New("no data points found")
	}

	// 将磁盘文件映射为读入内存的字节数组
	// 1. f.Fd()：映射文件
	// 2. 0：偏移量
	// 3. info.Size()：映射长度
	// 4. syscall.PROT_READ：磁盘只读
	// 5. syscall.MAP_SHARED：共享映射
	mmap, err := syscall.Mmap(int(f.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to perform mmap: %w", err)
	}

	// 将元数据读取
	// 将元数据读取到堆中
	meta := meta{}
	metaFile, err := os.Open(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	defer metaFile.Close()
	decoder := json.NewDecoder(metaFile)
	if err := decoder.Decode(&meta); err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}

	return &immutableChunk{
		dirPath:   dirPath,
		file:      metaFile,
		mmapData:  mmap,
		retention: retention,
		meta:      meta,
	}, nil
}

func (chunk *immutableChunk) insertRows(rows []Row) (outdatedRows []Row, err error) {
	return nil, fmt.Errorf("can't insert rows into immutable chunk")
}

func (chunk *immutableChunk) clean() error {
	if err := os.RemoveAll(chunk.dirPath); err != nil {
		return fmt.Errorf("failed to remove")
	}
	return nil
}

func (chunk *immutableChunk) selectDataPoints(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {
	if chunk.expired() {
		return nil, fmt.Errorf("the chunk is expired")
	}
	key := marshalKey(metric, labels)
	seriesIndex, ok := chunk.meta.SeriesMap[key]
	if !ok {
		return nil, errors.New("invalid partition")
	}
	// 1. 将内存映射的字节数组封装成一个 Reader（读取器），方便随机访问
	r := bytes.NewReader(chunk.mmapData)
	// 2. 将读取位置跳转（Seek）到该指标在数据文件中的起始偏移量（Offset）处
	if _, err := r.Seek(seriesIndex.Offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}
	// 3. 为该指标数据流创建一个新的序列解码器（Series Decoder）
	decoder, err := newSeriesDecoder(r)
	if err != nil {
		return nil, fmt.Errorf("failed to generate decoder for metric %q in %q: %w", key, chunk.dirPath, err)
	}

	// 4. 预先分配存储结果的切片，容量设置为该指标在该分区的总点数（避免频繁扩容）
	points := make([]*DataPoint, 0, seriesIndex.NumPoints)
	for i := 0; i < int(seriesIndex.NumPoints); i++ {
		point := &DataPoint{}

		// 从流中解码出一个数据点
		if err := decoder.decodePoint(point); err != nil {
			return nil, fmt.Errorf("解码分区 %q 中指标 %q 的数据点失败: %w", key, chunk.dirPath, err)
		}

		// 时间范围过滤：
		// 如果该点的时间戳小于查询的起始时间，则跳过（因为还没读到你要的范围）
		if point.Timestamp < start {
			continue
		}

		// 如果该点的时间戳大于或等于查询的截止时间，则终止（因为后面的点肯定更晚，不需要读了）
		if point.Timestamp >= end {
			break
		}

		// 将符合条件的数据点存入结果切片
		points = append(points, point)
	}
	return points, nil
}

func (chunk *immutableChunk) minTimestamp() int64 {
	return chunk.meta.MinTimestamp
}

func (chunk *immutableChunk) maxTimestamp() int64 {
	return chunk.meta.MaxTimestamp
}

func (chunk *immutableChunk) count() int64 {
	return chunk.meta.NumPoints
}

func (chunk *immutableChunk) active() bool {
	return false
}

func (chunk *immutableChunk) expired() bool {
	if chunk.retention < time.Since(chunk.meta.CreatedAt) {
		return true
	}
	return false
}
