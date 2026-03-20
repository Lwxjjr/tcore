package tcore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	timerpool "github.com/Lwxjjr/tcore/internal/timepool"
	_ "go.uber.org/automaxprocs"
)

var (
	defaultWorkersLimit = runtime.GOMAXPROCS(0)
	ErrNoDataPoints     = errors.New("no data points found")
)

// TimestampPrecision 时间精度
type TimestampPrecision string

const (
	Nanoseconds  TimestampPrecision = "ns"
	Microseconds TimestampPrecision = "us"
	Milliseconds TimestampPrecision = "ms"
	Seconds      TimestampPrecision = "s"

	defaultDuration     = 1 * time.Hour
	defaultRetention    = 168 * time.Hour
	defaultWriteTimeout = 15 * time.Second
	mutableChunkNum     = 2
)

type Storage interface {
	Reader
	InsertRows(rows []Row) error
	Close() error
}

// Reader 提供时序数据的读取访问、检索数据的 goroutine 安全功能
type Reader interface {
	Select(metric string, labels []Label, start, end int64) (points []*DataPoint, err error)
}

type storage struct {
	// chunkList 管理所有内存和磁盘分区
	chunkList *chunkList

	// duration 表示分区的时间跨度，超出时间会创建新分区
	duration time.Duration

	// retention 表示存储的时长，超出时长的磁盘分区会被删除
	retention time.Duration

	// timestampPrecision 指定时间精度，支持纳秒、微秒、毫秒、秒
	timestampPrecision TimestampPrecision

	// writeTimeout 控制写入超时时间
	writeTimeout time.Duration

	wg sync.WaitGroup

	// workersLimitCh 是一个并发限制通道，用于控制同时进行写入操作的 goroutine 数量
	workersLimitCh chan struct{}

	dataPath string

	// doneCh 通知所有 goroutine 退出
	doneCh chan struct{}
	// TODO
	// wirteTimeout 写入超时
	// logger 日志
}

func NewStorage() (Storage, error) {
	s := &storage{
		chunkList:      newChunkList(),
		duration:       defaultDuration,
		retention:      defaultRetention,
		writeTimeout:   defaultWriteTimeout,
		workersLimitCh: make(chan struct{}, defaultWorkersLimit),
		doneCh:         make(chan struct{}, 0),
	}

	if err := os.MkdirAll(s.dataPath, fs.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to make data directory %s: %w", s.dataPath, err)
	}

	return s, nil
}

func (s *storage) InsertRows(rows []Row) error {
	s.wg.Add(1)
	defer s.wg.Done()

	insert := func() error {
		defer func() { <-s.workersLimitCh }()
		if err := s.ensureActive(); err != nil {
			return err
		}
		iterator := s.chunkList.newIterator()
		n := s.chunkList.count()
		rowsToInsert := rows
		for i := 0; i < n && i < mutableChunkNum; i++ {
			if len(rowsToInsert) == 0 {
				break
			}
			if !iterator.next() {
				break
			}
			outdatedRows, err := iterator.chunk().insertRows(rowsToInsert)
			if err != nil {
				return fmt.Errorf("failed to insert rows: %w", err)
			}
			rowsToInsert = outdatedRows
		}
		return nil
	}
	// 限制并发 goroutine 的数量
	select {
	case s.workersLimitCh <- struct{}{}:
		return insert()
	default:
	}

	// 所有 worker 都很忙，最多等待 writeTimeout
	t := timerpool.Get(s.writeTimeout)
	select {
	case s.workersLimitCh <- struct{}{}:
		timerpool.Put(t)
		return insert()
	case <-t.C:
		timerpool.Put(t)
		return fmt.Errorf("failed to write a data point in %s, since it is overloaded with %d concurrent writers",
			s.writeTimeout, defaultWorkersLimit)
	}
}

func (s *storage) Select(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {

	return nil, nil
}

func (s *storage) Close() error {
	// TODO
	return nil
}

// ensureActive 确保有头部是活动分区
func (s *storage) ensureActive() error {
	headChunk := s.chunkList.getHead()
	if headChunk != nil && headChunk.active() {
		return nil
	}

	// 1. 所有分区都是不活动，添加一个新分区
	s.newChunk(nil)

	// 2. 异步刷新旧的分区到磁盘
	go func() {
		if err := s.flushChunk(); err != nil {
			fmt.Printf("failed to flush in-memory partitions: %v", err)
		}
	}()
	return nil
}

func (s *storage) newChunk(chunk chunk) {
	if chunk == nil {
		chunk = newMutableChunk(s.duration, s.timestampPrecision)
	}
	s.chunkList.insert(chunk)
}

// flushChunk 将准备好的持久化的 chunk 持久化到磁盘
//   - 保留的两个可写分区可以处理时间戳较早的乱序数据
//   - 磁盘分区使用 mmap 映射，只读
//   - 刷新过程中会创建新的磁盘分区目录（格式：p-{minTimestamp}-{maxTimestamp}）
//   - 每个磁盘分区包含 data 文件和 meta.json 文件
func (s *storage) flushChunk() error {
	i := 0
	iterator := s.chunkList.newIterator()
	for iterator.next() {
		if i < mutableChunkNum {
			i++
			continue
		}
		chunk := iterator.chunk()
		if chunk == nil {
			return fmt.Errorf("unexpected empty chunk found")
		}
		mutableChunk, ok := chunk.(*mutableChunk)
		if !ok {
			continue
		}

		// 尝试将可变块转换为不可变块
		// 磁盘分区将放置在内存分区所在的位置
		dir := filepath.Join(s.dataPath, fmt.Sprintf("p-%d-%d", mutableChunk.minTimestamp(), mutableChunk.maxTimestamp()))
		if err := s.flush(dir, mutableChunk); err != nil {
			return fmt.Errorf("failed to flush chunk into %s: %w", dir, err)
		}

		newChunk, err := openImmutableChunk(dir, s.retention)
		if errors.Is(err, ErrNoDataPoints) {
			if err := s.chunkList.remove(chunk); err != nil {
				return fmt.Errorf("failed to remove chunk: %w", err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to generate immutable chunk for %s: %w", dir, err)
		}
		if err := s.chunkList.swap(chunk, newChunk); err != nil {
			return fmt.Errorf("failed to swap chunks: %w", err)
		}
	}
	return nil
}

// flush 将内存分区的数据点压缩写入到指定目录
func (s *storage) flush(dirPath string, chunk *mutableChunk) error {
	if dirPath == "" {
		return fmt.Errorf("dirPath is required")
	}
	// 创建目标目录，用于存储
	if err := os.MkdirAll(dirPath, fs.ModePerm); err != nil {
		return fmt.Errorf("failed to make dir: %w", err)
	}

	// 创建 data 文件，用于存储压缩后的数据点
	f, err := os.Create(filepath.Join(dirPath, dataFileName))
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	// 创建序列编码器，用于将数据点编码写入文件
	encoder := newSeriesEncoder(f)

	// seriesMap 存储每个指标的元数据，包括名称、文件偏移量、时间范围等
	seriesMap := map[string]seriesIndex{}

	// 遍历内存分区中的所有指标，将每个指标的数据点写入磁盘
	chunk.seriesMap.Range(func(key, value interface{}) bool {
		s, ok := value.(*series)
		if !ok {
			fmt.Printf("unknown value found\n")
			return false
		}

		// 获取当前文件偏移量，记录该指标数据在文件中的起始位置
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			fmt.Printf("failed to set file offset of metric %q: %v\n", s.key, err)
			return false
		}

		// 将该指标的所有数据点（包括正常点和乱序点）编码并写入文件
		if err := s.encode(encoder); err != nil {
			fmt.Printf("failed to encode a data point that metric is %q: %v\n", s.key, err)
			return false
		}

		// 刷新编码器缓冲区，确保数据写入磁盘
		if err := encoder.flush(); err != nil {
			fmt.Printf("failed to flush data points that metric is %q: %v\n", s.key, err)
			return false
		}

		// 计算该指标的总数据点数量（正常点 + 乱序点）
		totoalNumPoints := s.count + int64(len(s.outOfOrderPoints))

		// 记录该指标的元数据，用于后续快速查询和数据定位
		seriesMap[s.key] = seriesIndex{
			Key:          s.key,
			Offset:       offset, // 在 data 文件中的起始位置
			MinTimestamp: s.minTimeStamp,
			MaxTimestamp: s.maxTimeStamp,
			NumPoints:    totoalNumPoints,
		}
		return true
	})

	// 创建分区元数据，包含分区信息和所有指标的索引
	data, err := json.Marshal(&meta{
		MinTimestamp: chunk.minTimestamp(),
		MaxTimestamp: chunk.maxTimestamp(),
		NumPoints:    chunk.count(),
		SeriesMap:    seriesMap,
		CreatedAt:    time.Now(),
	})

	// 应该最后写入 meta 文件，因为有效的 meta 文件存在证明磁盘分区是有效的
	// 如果在写入 meta 文件前程序崩溃，分区将被视为无效，不会被加载
	metaPath := filepath.Join(dirPath, metaFileName)
	if err := os.WriteFile(metaPath, data, fs.ModePerm); err != nil {
		return fmt.Errorf("failed to write metadata to %s: %w", metaPath, err)
	}

	return nil
}
