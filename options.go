package tcore

import "time"

// Option 是 NewStorage 的存储选项
type Option func(*storage)

// WithDataPath 指定存储时序数据的目录路径
// 默认为空字符串，意味着不会持久化任何数据
func WithDataPath(dataPath string) Option {
	return func(s *storage) {
		s.dataPath = dataPath
	}
}

// WithDuration 指定分区的时间戳范围
// 超出时间范围，就会插入新分区
// 默认为 1 小时
func WithDuration(duration time.Duration) Option {
	return func(s *storage) {
		s.duration = duration
	}
}

// WithRetention 配置数据保留时间
// 默认为 7 天
func WithRetention(retention time.Duration) Option {
	return func(s *storage) {
		s.retention = retention
	}
}

// WithTimestampPrecision 指定所有操作使用的时间戳精度。
// 默认为纳秒
func WithTimestampPrecision(precision TimestampPrecision) Option {
	return func(s *storage) {
		s.timestampPrecision = precision
	}
}

// WithWriteTimeout 指定 worker 忙碌时等待的超时时间。
//
// 存储限制并发 goroutine 的数量，以防止内存溢出错误和 CPU 争用，
// 即使有太多 goroutine 尝试写入也是如此。
//
// 默认为 30 秒。
func WithWriteTimeout(timeout time.Duration) Option {
	return func(s *storage) {
		s.writeTimeout = timeout
	}
}
