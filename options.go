package tcore

import "time"

// Options 定义数据库的可配置选项
type Options struct {
	// DirPath 数据存储目录路径
	DirPath string

	// MaxSegmentSize 单个 Segment 文件的最大大小，超过后轮转
	MaxSegmentSize int64

	// ForceFlushInterval 强制刷盘的时间间隔
	ForceFlushInterval time.Duration

	// BlockMaxPoints 触发刷盘的数据点数量阈值
	BlockMaxPoints int
}

// DefaultOptions 返回默认配置选项
func DefaultOptions() *Options {
	return &Options{
		DirPath:            "/tmp/bitcask-iot",
		MaxSegmentSize:     256 * 1024 * 1024, // 256MB
		ForceFlushInterval: 60 * time.Second,
		BlockMaxPoints:     1000,
	}
}

// Option 定义配置选项的函数类型
type Option func(*Options)

// WithDirPath 设置数据存储目录
func WithDirPath(path string) Option {
	return func(opts *Options) {
		opts.DirPath = path
	}
}

// WithMaxSegmentSize 设置单个 Segment 的最大大小
func WithMaxSegmentSize(size int64) Option {
	return func(opts *Options) {
		opts.MaxSegmentSize = size
	}
}

// WithForceFlushInterval 设置强制刷盘的时间间隔
func WithForceFlushInterval(interval time.Duration) Option {
	return func(opts *Options) {
		opts.ForceFlushInterval = interval
	}
}

// WithBlockMaxPoints 设置触发刷盘的数据点数量阈值
func WithBlockMaxPoints(maxPoints int) Option {
	return func(opts *Options) {
		opts.BlockMaxPoints = maxPoints
	}
}

// ApplyOptions 应用配置选项
func (opts *Options) ApplyOptions(options ...Option) {
	for _, opt := range options {
		opt(opts)
	}
}
