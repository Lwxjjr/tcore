package tcore

import (
	"sort"
	"strings"
)

const (
	maxLabelNameLen  = 256
	maxLabelValueLen = 16 * 1024
)

// Row 包含一个数据点以及用于标识一种指标的属性
type Row struct {
	Metric string  // 指标的唯一名称，必须设置此字段
	Labels []Label // 用于进一步详细标识的可选键值属性
	DataPoint
}

// DataPoint 表示一个数据点，是时序数据的最小单位
type DataPoint struct {
	Value     float64
	Timestamp int64
}

// Label 是一个时序标签
// 缺少名称或值的标签是无效的
type Label struct {
	Name  string
	Value string
}

func marshalKey(metric string, labels []Label) string {
	if len(labels) == 0 {
		return metric
	}

	// 1. 原地排序
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})

	// 2. 预估容量（稍微多给一点，抵消转义带来的长度增加）
	size := len(metric) + 2
	for _, l := range labels {
		size += len(l.Name) + len(l.Value) + 10 // 10 包含了 ="" 和 , 以及潜在的转义字符
	}

	// 使用 Grow 函数一次性分配内存
	var sb strings.Builder
	sb.Grow(size)

	sb.WriteString(metric)
	sb.WriteByte('{')

	for i, l := range labels {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(l.Name)
		sb.WriteString(`="`)

		// 核心优化：按字节遍历，手动转义，无额外内存分配
		for j := 0; j < len(l.Value); j++ {
			b := l.Value[j]
			switch b {
			case '"', '\\': // 处理引号和反斜杠
				sb.WriteByte('\\')
				sb.WriteByte(b)
			case '\n': // 处理换行符 (Prometheus 标准)
				sb.WriteString(`\n`)
			default:
				sb.WriteByte(b)
			}
		}

		sb.WriteByte('"')
	}
	sb.WriteByte('}')

	return sb.String()
}
