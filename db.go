package tcore

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ------------------------------------------------------------

// DB 是数据库的对外门面
// 它负责协调：Index (内存大脑) <-> Series (数据缓冲) <-> Storage (磁盘肌肉)
type DB struct {
	manager *Manager // 磁盘管理器
	idx     *Index   // 内存索引

	stopCh chan struct{}  // 关闭信号
	wg     sync.WaitGroup // 等待组 (确保后台任务安全退出)
}

// NewDB 🟢 1. 启动数据库
// dirPath: 数据存储目录 (会自动创建/加载 .vlog 文件)
func NewDB(dirPath string) (*DB, error) {
	mgr, _ := newManager(dirPath, 0)
	idx := NewIndex()

	// 🌟 1. 打开字典文件，并挂载到 Index 上，准备接收未来的新设备注册
	catalogPath := filepath.Join(dirPath, "catalog.idx")
	catalogFd, _ := os.OpenFile(catalogPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	idx.catalogFd = catalogFd

	// 🌟 2. 【开机第一步】：扫描 catalog.idx，恢复内存字典和 nextID 最大值！
	loadCatalog(catalogFd, idx)

	// 🌟 3. 【开机第二步】：扫描所有 .hint 文件。
	// 此时读出来的 Hint 只有 uint32，但你的大脑已经可以通过 idx.idToName 认识它们了！
	loadHintsFromDir(dirPath, idx)

	db := &DB{
		manager: mgr,
		idx:     idx,
		stopCh:  make(chan struct{}),
	}

	// 负责定期把长时间未写入的数据强制刷盘
	db.startWorker()

	return db, nil
}

// ➕ 补全极其简单的加载字典逻辑
func loadCatalog(file *os.File, idx *Index) {
	file.Seek(0, io.SeekStart) // 确保从头开始读
	maxID := uint32(0)

	for {
		id, name, err := DecodeCatalog(file)
		if err != nil {
			break // 读到 EOF 跳出
		}

		// 恢复正向和反向映射
		idx.seriesMap[name] = newSeries(id)
		idx.idToName[id] = name
		if id > maxID {
			maxID = id
		}
	}

	// 恢复自增 ID 的起点，防止重启后 ID 重复覆盖旧数据！
	if maxID > 0 {
		idx.nextID = maxID + 1
	}
}

// loadHintsFromDir 扫描数据目录，按字典序加载所有伴生索引文件
func loadHintsFromDir(dirPath string, idx *Index) error {
	pattern := filepath.Join(dirPath, "*.hint")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil // 没有 Hint 文件，直接放行
	}

	// 必须按时间字典序排列！保证内存里的 Meta 块是单调递增的
	sort.Strings(files)

	for _, file := range files {
		if err := processSingleHintFile(file, idx); err != nil {
			return err
		}
	}
	return nil
}

// processSingleHintFile 适配了全新的 uint32 定长解析！
func processSingleHintFile(filePath string, idx *Index) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	for {
		// 🌟 1. 极速解析：瞬间切下 38 字节，拿到的是 uint32 类型的 sensorID！
		sensorID, meta, err := DecodeHint(f)
		if err != nil {
			if err == io.EOF {
				break // 完美读完
			}
			return fmt.Errorf("Hint文件 %s 损坏: %v", filePath, err)
		}

		// 🌟 2. 核心联动：靠第一步读出来的 Catalog 字典，把 uint32 翻译回名字！
		idx.mu.RLock()
		name, ok := idx.idToName[sensorID]
		idx.mu.RUnlock()

		if !ok {
			// 极端容错防线：如果 Hint 里有数据，但字典里找不到对应的名字
			// 说明这批数据成了“孤儿”，直接跳过，防止引发恐慌 (Panic)
			// fmt.Printf("⚠️ 警告：发现孤儿区块，未知 SensorID: %d\n", sensorID)
			continue
		}

		// 🌟 3. 获取对应的设备 (因为前面 loadCatalog 已经把它放进内存了，这里绝对能拿到)
		s := idx.getOrCreateSeries(name)

		// 🌟 4. 把藏宝图挂载到设备的肚子里
		s.mu.Lock()
		s.blocks = append(s.blocks, meta)
		s.mu.Unlock()
	}

	return nil
}

// ==========================================
// 🚀 对外 API (Public API)
// ==========================================

// Write ✍️ 2. 写入数据
// 也就是 "存"：告诉我是谁、什么时候、多少度
func (db *DB) Write(sensorID string, timestamp int64, value float64) error {
	// 1. 封装成内部 Point
	point := Point{
		Time:  timestamp,
		Value: value,
	}

	// 2. 获取或创建 Series (内存中的专属通道)
	series := db.idx.getOrCreateSeries(sensorID)

	// 3. 尝试追加到内存 Buffer
	// ⚡️ 核心黑科技：如果 Buffer 满了，Series 会"窃取"满的那部分数据并返回给我们
	pointsToFlush := series.append(point)

	// 4. 如果发生了窃取，说明需要落盘了
	if len(pointsToFlush) > 0 {
		return db.flushSeriesData(series, pointsToFlush)
	}

	return nil
}

// Query 🔍 3. 查询数据
// 也就是 "取"：查出一段时间内的所有点
func (db *DB) Query(sensorID string, start, end int64) ([]Point, error) {
	// 1. 找设备
	series := db.idx.getOrCreateSeries(sensorID)
	if series == nil {
		return nil, nil // 没这个设备，直接返回空
	}

	var result []Point

	// 2. 查磁盘 (冷数据 Cold Data)
	// 从 Series 里拿出符合时间范围的"藏宝图坐标" (BlockMeta)
	blockMetas := series.findBlocks(start, end)

	for _, meta := range blockMetas {
		// 拿着坐标去问 Storage 要物理数据
		block, err := db.manager.readBlock(meta)
		if err != nil {
			return nil, fmt.Errorf("read block failed: %v", err)
		}

		// Block 只是粗略的块，需要过滤出精确符合时间范围的点
		for _, p := range block.Points {
			if p.Time >= start && p.Time <= end {
				result = append(result, p)
			}
		}
	}

	// 3. 查内存 (热数据 Hot Data)
	// 获取还没来得及落盘的数据
	hotData := series.getHotData()
	for _, p := range hotData {
		if p.Time >= start && p.Time <= end {
			result = append(result, p)
		}
	}

	return result, nil
}

// Keys 🔑 4. 获取所有 SensorID
func (db *DB) Keys() []string {
	return db.idx.getAllKeys()
}

// Close 🔴 5. 关闭数据库
// 安全退出，防止数据丢失
func (db *DB) Close() error {
	// 1. 通知后台协程停手
	close(db.stopCh)
	db.wg.Wait()

	// 2. (可选) 这里可以遍历所有 Series 执行一次强制 ForceFlush，确保内存不丢数据

	// 3. 关闭底层文件句柄
	return db.manager.close()
}

// ==========================================
// 🔒 内部胶水逻辑 (Internal Glue)
// ==========================================

// flushSeriesData 是连接 内存(Series) 和 磁盘(Storage) 的桥梁
func (db *DB) flushSeriesData(series *Series, points []Point) error {
	// 1. 组装 Block
	// DB 知道 series.ID()，也拿到了 points，所以由它来打包
	block := NewBlock(series.ID, points)

	// 2. 写磁盘
	// 这一步会发生：序列化 -> 压缩 -> 写文件 -> 可能触发文件切分(Rotate)
	meta, err := db.manager.writeBlock(block)
	if err != nil {
		return err
	}

	// 3. 拿回执
	// 把存储层返回的 BlockMeta (文件偏移量等) 挂回 Series 的索引链表上
	series.addBlockMeta(meta)

	return nil
}

// ==========================================
// ⏰ 后台任务 (Background Worker)
// ==========================================

func (db *DB) startWorker() {
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		// 每秒巡逻一次
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-db.stopCh:
				return
			case <-ticker.C:
				db.checkForceFlush()
			}
		}
	}()
}

// checkForceFlush 巡检所有 Series，看谁的数据太久没刷盘
func (db *DB) checkForceFlush() {
	allSeries := db.idx.getAllSeries()
	for _, series := range allSeries {
		// Series 内部会判断：如果数据存在且超过 60秒 未刷盘，就返回数据
		if points := series.checkForTicker(); len(points) > 0 {
			// 复用核心刷盘逻辑
			if err := db.flushSeriesData(series, points); err != nil {
				fmt.Printf("Error flushing series %d: %v\n", series.ID, err)
			}
		}
	}
}
