package fsimage

import (
	"testing"
)

func TestCalculatePath(t *testing.T) {
	ctx := NewPass1Context()
	ctx.IDToName[16386] = "user"
	ctx.IDToName[16387] = "hive"

	ctx.ChildToParent[16386] = RootInodeID
	ctx.ChildToParent[16387] = 16386
	ctx.ChildToParent[16388] = 16387

	img := &FSImage{}

	tests := []struct {
		id   uint64
		name string
		want string
	}{
		{RootInodeID, "", "/"},
		{16386, "user", "/user"},
		{16387, "hive", "/user/hive"},
		{16388, "warehouse", "/user/hive/warehouse"},
	}

	for _, tt := range tests {
		got := img.calculatePath(ctx, tt.id, tt.name)
		if got != tt.want {
			t.Errorf("calculatePath(%d, %s) = %s; want %s", tt.id, tt.name, got, tt.want)
		}
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
