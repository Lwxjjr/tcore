package tcore

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// defaultSegmentMaxSize 单个 Segment 的默认最大大小（256MB）
const defaultSegmentMaxSize = 256 * 1024 * 1024

// Manager 负责管理多个数据段文件
type Manager struct {
	mu            sync.RWMutex
	dirPath       string
	activeSegment *Segment
	olderSegments map[uint32]*Segment
	maxSize       int64 // 单个 Segment 的最大大小，超过则轮转
}

// NewManager 初始化并加载现有的段文件
func newManager(dirPath string, maxSize int64) (*Manager, error) {
	if maxSize == 0 {
		maxSize = defaultSegmentMaxSize
	}
	mgr := &Manager{
		dirPath:       dirPath,
		olderSegments: make(map[uint32]*Segment),
		maxSize:       maxSize,
	}

	// 确保目录存在
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create data directory: %v", err)
		}
	}

	if err := mgr.loadSegments(); err != nil {
		return nil, err
	}

	return mgr, nil
}

// loadSegments 扫描目录并加载已有的文件
func (m *Manager) loadSegments() error {
	files, err := os.ReadDir(m.dirPath)
	if err != nil {
		return err
	}

	var ids []uint32
	for _, f := range files {
		// 🌟 巧妙之处：只认 .vlog 文件来提取 ID 即可
		if strings.HasPrefix(f.Name(), SegmentFileNamePrefix) && strings.HasSuffix(f.Name(), SegmentFileNameSuffix) {
			idStr := strings.TrimPrefix(strings.TrimSuffix(f.Name(), SegmentFileNameSuffix), SegmentFileNamePrefix)
			id, err := strconv.ParseUint(idStr, 10, 32)
			if err == nil {
				ids = append(ids, uint32(id))
			}
		}
	}

	if len(ids) == 0 {
		return m.rotate(0) // 如果没有任何文件，开创 0 号纪元
	}

	// 排序保证老文件在前
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for i := 0; i < len(ids)-1; i++ {
		// 🛠️ 修改：直接传目录给 newSegment，它会自动搞定 vlog 和 hint
		seg, err := newSegment(m.dirPath, ids[i])
		if err != nil {
			return err
		}
		m.olderSegments[ids[i]] = seg
	}

	// 处理最新的活跃 Segment
	lastID := ids[len(ids)-1]
	// 🛠️ 修改：直接传目录
	seg, err := newSegment(m.dirPath, lastID)
	if err != nil {
		return err
	}
	m.activeSegment = seg

	return nil
}

// WriteBlock 接收业务块，编码并处理文件轮转，然后写入底层
func (m *Manager) writeBlock(block *Block) (*BlockMeta, error) {
	// 1. 🚀 锁外操作：执行 CPU 密集的序列化
	data, err := block.encode()
	if err != nil {
		return nil, err
	}
	dataSize := int64(len(data))

	// 2. ⚡️ 获取当前活跃分片的指针
	m.mu.RLock()
	activeSeg := m.activeSegment
	m.mu.RUnlock()

	// 3. 🎯 预判轮转 (预测：当前大小 + 新数据大小 > 最大限制)
	if activeSeg.size()+dataSize > m.maxSize {
		m.mu.Lock()
		// Double-Check：防止其他并发协程已经完成了轮转
		if m.activeSegment == activeSeg {
			if err := m.rotate(activeSeg.ID + 1); err != nil {
				m.mu.Unlock()
				return nil, err
			}
		}
		// 指向最新的 Segment
		activeSeg = m.activeSegment
		m.mu.Unlock()
	}

	// 4. 💾 纯物理写入 (Manager 不加锁，锁在 activeSeg 内部)
	offset, err := activeSeg.write(data)
	if err != nil {
		return nil, err
	}

	// 5. 🧾 组装元数据返回给上层
	meta := block.toMeta(activeSeg.ID, offset, uint32(dataSize))

	if err := WriteHintRecord(activeSeg.HintFile, block.SensorID, meta); err != nil {
		// 这里只打印错误不 return，因为真实数据已经落盘了，避免上层收到假报错
		// logger.Errorf("写入 Hint 伴生文件失败: %v", err)
	}

	return meta, nil
}

// ReadBlock 根据 FileID 找到对应的 Segment 并读取解包
func (m *Manager) readBlock(meta *BlockMeta) (*Block, error) {
	m.mu.RLock()
	var seg *Segment
	if m.activeSegment != nil && m.activeSegment.ID == meta.FileID {
		seg = m.activeSegment
	} else {
		seg = m.olderSegments[meta.FileID]
	}
	m.mu.RUnlock()

	if seg == nil {
		return nil, fmt.Errorf("segment %d not found", meta.FileID)
	}

	// 调用底层物理读取
	data, err := seg.readAt(meta.Size, meta.Offset)
	if err != nil {
		return nil, err
	}

	// 锁外执行反序列化 (依赖 block.go 中的 DecodeBlock)
	return decodeBlock(data)
}

// rotate 封存当前活跃段，开启一个新段
func (m *Manager) rotate(nextID uint32) error {
	if m.activeSegment != nil {
		// 🌟 关键保命动作：退役前必须强制刷盘！
		// 这里得益于我们之前的改造，它会自动把旧的 .vlog 和 .hint 一起安全刷盘
		if err := m.activeSegment.Sync(); err != nil {
			return fmt.Errorf("failed to sync segment %d: %v", m.activeSegment.ID, err)
		}
		m.olderSegments[m.activeSegment.ID] = m.activeSegment
	}

	// 🛠️ 核心修改：不再单独拼装 path，直接把 m.dirPath 交给 newSegment！
	// 让 newSegment 自己在里面把 .vlog 和 .hint 安排得明明白白
	seg, err := newSegment(m.dirPath, nextID)
	if err != nil {
		return err
	}

	m.activeSegment = seg
	return nil
}

// Close 关闭所有段文件
func (m *Manager) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeSegment != nil {
		// 关闭前也要刷一次盘
		m.activeSegment.Sync()
		if err := m.activeSegment.close(); err != nil {
			return err
		}
	}

	for _, seg := range m.olderSegments {
		if err := seg.close(); err != nil {
			return err
		}
	}
	return nil
}
