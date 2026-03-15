package tcore

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// hintRecordSize 绝对定长设计：带来极致的序列化与扫盘性能
	// 4(SensorID) + 4(FileID) + 8(Min) + 8(Max) + 8(Offset) + 4(Size) + 2(Count) = 38 bytes
	hintRecordSize = 38
)

var ErrHintCorrupted = errors.New("hint file is corrupted or truncated")

// ==========================================
// 1. 核心读写动作封装 (面向 Interface 编程，完美解耦)
// ==========================================

// WriteHintRecord 供 Manager 落盘时调用。直接写入 38 字节定长数据
func WriteHintRecord(w io.Writer, sensorID uint32, meta *BlockMeta) error {
	hintBuf := EncodeHint(sensorID, meta)
	_, err := w.Write(hintBuf)
	return err
}

// DecodeHint 供 DB 开机扫盘时调用。每次严格切出 38 字节，绝不多读或少读
func DecodeHint(r io.Reader) (uint32, *BlockMeta, error) {
	buf := make([]byte, hintRecordSize)

	// 死等 38 个字节，读不满就报错
	if _, err := io.ReadFull(r, buf); err != nil {
		if err == io.EOF {
			return 0, nil, io.EOF // 正常读到文件末尾
		}
		return 0, nil, ErrHintCorrupted // 遇到断电导致的半截脏数据
	}

	// 精准切割 38 字节
	sensorID := binary.BigEndian.Uint32(buf[0:4])
	meta := &BlockMeta{
		FileID:  binary.BigEndian.Uint32(buf[4:8]),
		MinTime: int64(binary.BigEndian.Uint64(buf[8:16])),
		MaxTime: int64(binary.BigEndian.Uint64(buf[16:24])),
		Offset:  int64(binary.BigEndian.Uint64(buf[24:32])),
		Size:    binary.BigEndian.Uint32(buf[32:36]),
		Count:   binary.BigEndian.Uint16(buf[36:38]),
	}

	return sensorID, meta, nil
}

// ==========================================
// 2. 底层序列化协议 (纯定长，无任何动态计算)
// ==========================================

// EncodeHint 将 uint32 的 SensorID 和 Meta 序列化为 38 字节的二进制切片
func EncodeHint(sensorID uint32, meta *BlockMeta) []byte {
	buf := make([]byte, hintRecordSize)

	// 1. 写入 SensorID (4 字节)
	binary.BigEndian.PutUint32(buf[0:4], sensorID)

	// 2. 写入 Meta (34 字节)
	binary.BigEndian.PutUint32(buf[4:8], meta.FileID)
	binary.BigEndian.PutUint64(buf[8:16], uint64(meta.MinTime))
	binary.BigEndian.PutUint64(buf[16:24], uint64(meta.MaxTime))
	binary.BigEndian.PutUint64(buf[24:32], uint64(meta.Offset))
	binary.BigEndian.PutUint32(buf[32:36], meta.Size)
	binary.BigEndian.PutUint16(buf[36:38], meta.Count)

	return buf
}
