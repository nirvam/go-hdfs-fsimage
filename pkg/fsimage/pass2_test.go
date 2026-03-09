package fsimage

import (
	"testing"

	"github.com/nirvam/go-hdfs-fsimage/pkg/exporter"
	"github.com/nirvam/go-hdfs-fsimage/pkg/fsimage/types"
	"google.golang.org/protobuf/proto"
)

func TestGetDirPath(t *testing.T) {
	ctx := NewPass1Context()
	ctx.IDToName[16386] = "user"
	ctx.IDToName[16387] = "hive"
	ctx.IDToName[16388] = "warehouse"

	ctx.ChildToParent[16386] = RootInodeID
	ctx.ChildToParent[16387] = 16386
	ctx.ChildToParent[16388] = 16387

	img := &FSImage{}

	tests := []struct {
		id   uint64
		want string
	}{
		{RootInodeID, ""},
		{16386, "/user"},
		{16387, "/user/hive"},
		{16388, "/user/hive/warehouse"},
	}

	for _, tt := range tests {
		got := img.getDirPath(ctx, tt.id)
		if got != tt.want {
			t.Errorf("getDirPath(%d) = %s; want %s", tt.id, got, tt.want)
		}
		// Verify caching
		if tt.id != RootInodeID {
			if cached, ok := ctx.DirPathCache[tt.id]; !ok || cached != tt.want {
				t.Errorf("DirPathCache[%d] = %s; want %s", tt.id, cached, tt.want)
			}
		}
	}
}

func TestFillINodeRecordPath(t *testing.T) {
	ctx := NewPass1Context()
	ctx.IDToName[16386] = "user"
	ctx.ChildToParent[16386] = RootInodeID

	img := &FSImage{}

	record := &exporter.INodeRecord{}
	inode := &types.INodeSection_INode{
		Id:   proto.Uint64(16386),
		Name: []byte("user"),
		Type: types.INodeSection_INode_DIRECTORY.Enum(),
	}

	img.fillINodeRecord(ctx, record, inode)
	if record.Path != "/user" {
		t.Errorf("fillINodeRecord path = %s; want /user", record.Path)
	}

	// Test nested file
	fileRecord := &exporter.INodeRecord{}
	fileInode := &types.INodeSection_INode{
		Id:   proto.Uint64(1000),
		Name: []byte("data.bin"),
		Type: types.INodeSection_INode_FILE.Enum(),
		File: &types.INodeSection_INodeFile{},
	}
	ctx.ChildToParent[1000] = 16386

	img.fillINodeRecord(ctx, fileRecord, fileInode)
	if fileRecord.Path != "/user/data.bin" {
		t.Errorf("fillINodeRecord path = %s; want /user/data.bin", fileRecord.Path)
	}
}

func TestFormatMode(t *testing.T) {
	tests := []struct {
		mode  uint16
		isDir bool
		want  string
	}{
		{0755, true, "drwxr-xr-x"},
		{0644, false, "-rw-r--r--"},
		{0700, true, "drwx------"},
		{01777, true, "drwxrwxrwt"}, // Sticky bit
		{01666, true, "drw-rw-rwT"}, // Sticky bit without execute
	}

	for _, tt := range tests {
		got := formatMode(tt.mode, tt.isDir)
		if got != tt.want {
			t.Errorf("formatMode(%o, %v) = %s; want %s", tt.mode, tt.isDir, got, tt.want)
		}
	}
}
