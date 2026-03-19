package tcore

import (
	"sync"
	"time"
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
	chunkList *chunkList    // chunkList 管理所有内存和磁盘分区
	duration  time.Duration // 分区的时间跨度，超出时间会创建新分区
	retention time.Duration // 存储的时长，超出时长的磁盘分区会被删除

	wg     sync.WaitGroup
	doneCh chan struct{} // doneCh 通知所有 goroutine 退出
	// TODO
	// wirteTimeout 写入超时
	// logger 日志

}

func NewStorage() (Storage, error) {

}
