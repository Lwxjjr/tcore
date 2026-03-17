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

// marshalKey 通过编码标签来构建唯一的字节。
func marshalKey(metric string, labels []Label) string {
	// 如果没有标签，直接返回指标名称
	if len(labels) == 0 {
		return metric
	}

	// 复制并排序标签以确保生成的键是确定性的（即相同的标签组合产生相同的字符串）
	sortedLabels := make([]Label, len(labels))
	copy(sortedLabels, labels)
	sort.Slice(sortedLabels, func(i, j int) bool {
		return sortedLabels[i].Name < sortedLabels[j].Name
	})

	// 使用 strings.Builder 高效构建字符串
	var sb strings.Builder
	sb.WriteString(metric)
	for _, l := range sortedLabels {
		// 使用 \x00 分隔标签名，\x01 分隔标签值，确保编码的唯一性
		sb.WriteByte('\x00')
		sb.WriteString(l.Name)
		sb.WriteByte('\x01')
		sb.WriteString(l.Value)
	}
	return sb.String()
}
