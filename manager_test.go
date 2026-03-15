package tcore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestManager_Rotate(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-mgr-test")
	defer os.RemoveAll(dir)

	// 设置极小的 maxSize 以触发轮转 (100 字节)
	mgr, err := newManager(dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.close()

	// 写入第一个块
	b1 := &Block{SensorID: 1, Points: []Point{{Time: 1, Value: 1.1}}}
	meta1, err := mgr.writeBlock(b1)
	if err != nil {
		t.Fatal(err)
	}
	if meta1.FileID != 0 {
		t.Errorf("expected FileID 0, got %d", meta1.FileID)
	}

	// 写入第二个块，触发轮转
	// 这里我们需要确保写入的数据超过 100 字节，或者多次写入
	for i := 0; i < 10; i++ {
		b := &Block{SensorID: 1, Points: []Point{{Time: int64(i), Value: float64(i)}}}
		_, err = mgr.writeBlock(b)
		if err != nil {
			t.Fatal(err)
		}
	}

	// 检查当前 Active ID 是否增加了
	if mgr.activeSegment.ID == 0 {
		t.Error("expected rotation, but active ID is still 0")
	}

	// 验证旧数据依然可读
	readB1, err := mgr.readBlock(meta1)
	if err != nil {
		t.Fatal(err)
	}
	if readB1.Points[0].Value != 1.1 {
		t.Errorf("data mismatch, expected 1.1, got %f", readB1.Points[0].Value)
	}
}

func TestManager_Reload(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-mgr-reload")
	defer os.RemoveAll(dir)

	// 1. 第一轮写入
	mgr1, _ := newManager(dir, 1024*1024)
	b1 := &Block{SensorID: 99, Points: []Point{{Time: 123, Value: 45.6}}}
	mgr1.writeBlock(b1)
	mgr1.close()

	// 2. 检查文件是否存在
	expectedFile := filepath.Join(dir, "seg-000000.vlog")
	if _, err := os.Stat(expectedFile); os.IsNotExist(err) {
		t.Fatal("expected segment file not found")
	}

	// 3. 重新加载
	mgr2, err := newManager(dir, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr2.close()

	if mgr2.activeSegment.ID != 0 {
		t.Errorf("expected active ID 0, got %d", mgr2.activeSegment.ID)
	}
}

func TestNewManager_CreateDir(t *testing.T) {
	// 定义一个不存在的临时子目录
	baseDir, _ := os.MkdirTemp("", "mgr-create-dir")
	defer os.RemoveAll(baseDir)

	targetDir := filepath.Join(baseDir, "nested/tsdb_data")

	// 确保目标目录当前不存在
	if _, err := os.Stat(targetDir); !os.IsNotExist(err) {
		t.Fatalf("target directory %s should not exist before test", targetDir)
	}

	// 初始化 Manager
	mgr, err := newManager(targetDir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create Manager with non-existent directory: %v", err)
	}
	defer mgr.close()

	// 验证目录是否已创建
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		t.Error("expected directory to be created, but it does not exist")
	}
}
