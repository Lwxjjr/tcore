// core/catalog.go
package tcore

import (
	"encoding/binary"
	"io"
)

// WriteCatalogRecord 记录新生儿诞生：[ID:4字节] + [名字长度:2字节] + [名字内容]
func WriteCatalogRecord(w io.Writer, id uint32, name string) error {
	nameLen := len(name)
	buf := make([]byte, 6+nameLen)

	binary.BigEndian.PutUint32(buf[0:4], id)
	binary.BigEndian.PutUint16(buf[4:6], uint16(nameLen))
	copy(buf[6:], name)

	_, err := w.Write(buf)
	return err
}

// DecodeCatalog 开机扫盘专用：恢复字典映射
func DecodeCatalog(r io.Reader) (uint32, string, error) {
	header := make([]byte, 6)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, "", err // 包含 io.EOF
	}

	id := binary.BigEndian.Uint32(header[0:4])
	nameLen := binary.BigEndian.Uint16(header[4:6])

	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(r, nameBuf); err != nil {
		return 0, "", err
	}

	return id, string(nameBuf), nil
}
