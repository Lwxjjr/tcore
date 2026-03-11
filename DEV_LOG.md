# Bitcask IoT 开发日志

## 2026-03-11

### 🏛️ 架构反思：借鉴 tstorage 的扁平化分区思路

在研究高性能 TSDB 库 `tstorage` 后，对比了 `bitcask-iot` 当前的“传感器中心化”架构，发现了在大规模场景下的潜在瓶颈。

#### 1. 架构对比示意图

**【当前：传感器中心化 (Sensor-Centric)】**
```text
[ DB (Entry) ]
  │
  ├── [ Index (Global) ]
  │     ├── Series "Sensor_A" ── [ activeBuffer (Hot) ] + [ BlockMetas (Cold Index) ]
  │     ├── Series "Sensor_B" ── [ activeBuffer (Hot) ] + [ BlockMetas (Cold Index) ]
  │     └─ ... (10,000+ Series)
  └── [ Manager (Physical) ] ── [ Segment.vlog ] (物理追加)
```

**【目标：时间分片中心化 (Time-Partitioned / tstorage Style)】**
```text
[ Storage (Entry) ]
  └── [ PartitionList (Time-Ordered) ]
        ├── Node 1: [ MemoryPartition (Active/Hot) ]  <-- 全局写入点 (Map[ID][]Point)
        ├── Node 2: [ DiskPartition (ReadOnly/Cold) ] <-- 压缩后的磁盘映射 (mmap)
        └── Node 3: [ DiskPartition (ReadOnly/Cold) ]
```

#### 2. 核心差异与现有问题分析

*   **内存碎片 (Memory Bloat)**：
    *   *现状*：10,000 个传感器对应 10,000 个 `activeBuffer` 切片。由于 Go slice 的扩容机制和碎片化，小传感器多时内存浪费严重。
    *   *优化*：改用 `MemoryPartition` 后，所有传感器在同一时间窗口内共享大 Map，内存更连续，GC 友好。
*   **落盘碎片化 (Flush Complexity)**：
    *   *现状*：按传感器落盘，导致 `.vlog` 文件中 Block 极其细碎（不同传感器交织），读取历史数据时 IOPS 极高。
    *   *优化*：按时间整块落盘，一小时内所有数据一次性顺序写入，大幅提升磁盘吞吐。
*   **过期删除 (Retention) 困境**：
    *   *现状*：删除旧数据需遍历所有 `Series` 对象去清理 `BlockMetas`，复杂度 O(N*M)。
    *   *优化*：只需从 `PartitionList` 尾部移除对应的 `DiskPartition` 节点并物理删除文件，复杂度 O(1)。
*   **状态竞争 (Lock Contention)**：
    *   *现状*：读写/查询 Series 需频繁竞争对象锁，且 `blocks` 列表随时间无限增长。
    *   *优化*：`DiskPartition` 是不可变的（Immutable），查询冷数据无锁化；仅 `MemoryPartition` 需要写入锁。

#### 3. 下一步演进思路 (The Go Way)

你的架构目前的定位： 这是一个非常好的 Bitcask 模型变种。它的优点是“读某个传感器的最新数据极快”，因为它直接在Series 里就定位到了。


如果你要面对的是真实的 IoT 场景（高频写入、海量设备、有过期需求）：
   1. 引入 Partition 概念：不要让 Series 管理冷数据索引，让 Partition 管理。
   2. 扁平化 Series：Series 应该只是一段逻辑逻辑 ID，不应该是一个沉重的管理类。
   3. 接口化存储：像 tstorage 一样定义 Partition 接口，让 DB 面对的是“一组时间分片”，而不是“一堆传感器”。

1.  **定义 `Partition` 接口**：解耦“内存形态”与“磁盘形态”，通过行为驱动而非结构嵌套。
2.  **引入 `PartitionList`**：作为 `DB` 的核心数据容器，管理分区的生命周期。
3.  **实现 Swap 机制**：后台异步将 `MemoryPartition` 转换为 `DiskPartition` 并实现原子替换。

---

## 2026-2-27



---

## 2026-02-10

### 🎯 今日核心任务：构建高效刷入机制与架构重构

#### 1. Series 内部：状态监控与"交出"逻辑
*   **Flush 条件**：`Series.ShouldFlush()` 判断缓冲区是否达到阈值（点数 >= 1000 或时间 >= 60s）。
*   **解耦设计**：`Series.Flush()` 直接返回 `*Block`，不持有 `Segment` 引用。由 Engine 调用 `Manager.WriteBlock()` 处理写入和轮转。
*   **索引更新**：写入完成后，调用 `Series.AddBlock(meta)` 将 `BlockMeta` 追加到索引列表。

#### 2. Manager 内部：物理策略与"追加"逻辑
*   **原子追加与轮转检查**：在 `Manager.WriteBlock` 中，首先检查物理文件状态（大小是否 > 256MB 或 创建时间是否 > 2小时）。如果满足，立即轮转文件。
*   **顺序写保证**：通过 `Manager.mu` 确保即便有 100 个 Series 同时请求刷盘，进入磁盘文件的 Block 也是排队顺序写入的，维持磁盘高吞吐。
*   **时间戳记录**：为每个 Segment 记录 `CreatedAt`，解决你担心的"一个文件跨越数天"的问题。

#### 3. Engine 内部：心跳驱动与"调度"逻辑
*   **中央心跳 (1s Ticker)**：在 `Engine` 层启动一个全局协程。
*   **惰性扫描**：每秒钟 Ticker 醒来，遍历 `Index` 中的所有 `Series`，仅对 `ShouldFlush()` 返回 `true` 的 `Series` 发起真正的 `Flush(Manager)` 调用。
*   **空载优化**：如果 Buffer 为空，直接跳过，确保在没有写入时系统处于零 IO 状态。

---

## 2026-02-09

### ✅ 新增完成的功能

* **Segment 轮转管理**：实现了 `SegmentManager` 完整的轮转机制
    - 自动检测文件大小，超过阈值（512MB）自动切换新文件
    - 支持启动时加载已有的 Segment 文件（按 ID 排序）
    - 区分 Active Segment（可写）和 Older Segments（只读）
    - 提供统一的 `WriteBlock` 和 `ReadBlock` 接口
* **文件生命周期管理**：完善了 Segment 的打开、写入、读取、关闭流程
    - `NewSegment`: 支持创建新文件或追加写入已有文件
    - `ReadBlock`: 使用 `ReadAt` 实现并发安全的随机读取
    - `Close`: 统一关闭所有文件句柄，防止资源泄漏

### 📊 与 2 月 6 日的主要差异

| 功能模块 | 2 月 6 日状态 | 2 月 9 日状态 | 进展 |
|---------|-------------|-------------|------|
| Block 定义 | ✅ 已实现 | ✅ 完成 | 无变化 |
| Segment 文件 | ✅ 基础实现 | ✅ 完成 | 新增并发读取、关闭管理 |
| Manager 轮转 | ❌ 待实现 | ✅ 完成 | 新增自动轮转、文件加载 |
| Index 管理 | ✅ 已实现 | ✅ 完成 | 无变化 |
| Series 缓冲 | ✅ 已实现 | ✅ 完成 | 无变化 |

### 🔧 技术细节更新

* **并发模型优化**：
    - `SegmentManager` 使用 `sync.RWMutex` 保护 `activeSegment` 和 `olderSegments` 的访 问
    - `Segment.ReadBlock` 使用 `ReadAt` 支持并发读取（只读锁）
    - `Segment.WriteBlock` 使用互斥锁保证写入原子性
* **文件命名规范**：
    - 采用 `seg-000001.vlog` 格式，6 位数字 ID 便于排序
    - 提供统一的 `GetSegmentPath` 工具函数
* **内存索引关联**：
    - `BlockMeta` 包含 `FileID` 字段，关联到具体的 Segment 文件
    - 支持跨文件的 Block 读取，`ReadBlock` 自动定位到对应的 Segment

### 🎯 待完成的下一步

1. **引擎协调器 (`internal/engine`)** - 【最高优先级】
    - 整合 Index 和 Storage Manager
    - 实现 Put 接口：接收数据 -> 查找/创建 Series -> 追加 Buffer -> 触发 Flush
    - 实现 Get 接口：查询数据 -> 遍历 Blocks -> 读取 Segment -> 解压返回
2. **查询层 (`internal/query`)** - 【高优先级】
    - 实现时间范围查询（二分查找 Blocks）
    - 数据解压和过滤
    - 降采样算法（LTTB）
3. **崩溃恢复 (Recovery)** - 【中优先级】
    - 启动时扫描 Segment 重建 Index
    - 或实现 WAL 机制保证数据安全

---

## 2026-02-06

### 🔍 目前的代码状态

* **垂直路径已打通**：写入数据点 -> 存入内存 -> 满足条件 -> 打包刷入磁盘 -> 返回索引项。
* **并发安全**：在所有关键路径（Index 查找、Buffer 追加、文件写入、文件并发读）都使用了 `sync.RWMutex` 或双重检查锁。
* **存储能力**：实现了基于 `gob` 的 Block 序列化与 `Segment` 文件的 `ReadAt` 并发读取。

### 💡 数据结构重构重点 (Bitcask -> IoT)

*   **聚合：从"点"到"块"**：
    放弃单点存储，改为 **Block (块)** 存储。内存索引不再记录每个数据点，而是记录块的时间范围 and 物理位置。内存消耗降低 99%，支持快速范围查询。
*   **压缩：String -> uint32 ID**：
    引入 ID 映射机制。磁盘不再存储冗长的传感器名称，统一使用 4 字节 ID。在万级传感器场景下，大幅减少存储冗余。
*   **进化：Hash -> 稀疏索引**：
    将 Bitcask 原始的哈希索引改为 **时间线稀疏索引 (BlockMeta)**。利用二分查找快速定位磁盘块，解决 KV 引擎无法处理时间范围检索的问题。

### 🚧 接下来的重点（待完成）

1. **Segment 轮转管理**：实现 `Manager` 结构，当文件写满（如 512MB）时自动切换新文件。
2. **引擎协调器 (`internal/engine`)**：初始化各组件，配置路径，管理全局生命周期。
3. **崩溃恢复 (Recovery)**：实现 WAL 或通过扫描 Segment 重建内存索引。
