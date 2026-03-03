package fsimage

import (
	"strings"
	"sync"
	"time"

	"github.com/nirvam/go-hdfs-fsimage/pkg/exporter"
	"github.com/nirvam/go-hdfs-fsimage/pkg/fsimage/types"
)

var (
	pathSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]string, 0, 32)
		},
	}
)

const (
	RootInodeID = 16385
)

func (img *FSImage) RunPass2(ctx *Pass1Context, exp exporter.Exporter) error {
	r, err := img.OpenSection("INODE")
	if err != nil {
		return err
	}
	defer r.Close()

	// Read header
	header := &types.INodeSection{}
	if err := ReadDelimited(r, header); err != nil {
		return err
	}

	numINodes := header.GetNumInodes()
	inode := &types.INodeSection_INode{}
	for i := uint64(0); i < numINodes; i++ {
		inode.Reset()
		if err := ReadDelimited(r, inode); err != nil {
			return err
		}

		record := exporter.InodeRecordPool.Get().(*exporter.INodeRecord)
		record.Reset()
		img.fillINodeRecord(ctx, record, inode)

		if err := exp.Export(record); err != nil {
			exporter.InodeRecordPool.Put(record)
			return err
		}
		exporter.InodeRecordPool.Put(record)
	}

	return nil
}

func (img *FSImage) fillINodeRecord(ctx *Pass1Context, record *exporter.INodeRecord, inode *types.INodeSection_INode) {
	id := inode.GetId()
	name := string(inode.GetName())

	record.ID = id
	record.Type = inode.GetType().String()

	// Calculate Path
	record.Path = img.calculatePath(ctx, id, name)

	// Process details based on type
	switch inode.GetType() {
	case types.INodeSection_INode_FILE:
		f := inode.GetFile()
		record.Replication = f.GetReplication()
		record.ModificationTime = time.UnixMilli(int64(f.GetModificationTime()))
		record.AccessTime = time.UnixMilli(int64(f.GetAccessTime()))
		record.PreferredBlockSize = f.GetPreferredBlockSize()
		record.BlocksCount = len(f.GetBlocks())

		var size uint64
		for _, b := range f.GetBlocks() {
			size += b.GetNumBytes()
		}
		record.FileSize = size
		img.fillPermission(ctx, record, f.GetPermission())

	case types.INodeSection_INode_DIRECTORY:
		d := inode.GetDirectory()
		record.ModificationTime = time.UnixMilli(int64(d.GetModificationTime()))
		img.fillPermission(ctx, record, d.GetPermission())

	case types.INodeSection_INode_SYMLINK:
		s := inode.GetSymlink()
		record.ModificationTime = time.UnixMilli(int64(s.GetModificationTime()))
		record.AccessTime = time.UnixMilli(int64(s.GetAccessTime()))
		img.fillPermission(ctx, record, s.GetPermission())
	}
}

func (img *FSImage) calculatePath(ctx *Pass1Context, id uint64, name string) string {
	if id == RootInodeID {
		return "/"
	}

	parts := pathSlicePool.Get().([]string)
	parts = parts[:0]
	defer func() {
		pathSlicePool.Put(parts)
	}()

	if name != "" {
		parts = append(parts, name)
	}

	curr := id
	for {
		parent, ok := ctx.ChildToParent[curr]
		if !ok || parent == 0 {
			break
		}
		if parent == RootInodeID {
			break
		}

		pName, ok := ctx.IDToName[parent]
		if ok && pName != "" {
			parts = append(parts, pName)
		}
		curr = parent
	}

	if len(parts) == 0 {
		return "/"
	}

	// Calculate total length for pre-allocation
	totalLen := 1 // Initial /
	for _, p := range parts {
		totalLen += len(p) + 1
	}

	var sb strings.Builder
	sb.Grow(totalLen)

	// Write in reverse order
	for i := len(parts) - 1; i >= 0; i-- {
		sb.WriteByte('/')
		sb.WriteString(parts[i])
	}

	return sb.String()
}

func (img *FSImage) fillPermission(ctx *Pass1Context, record *exporter.INodeRecord, perm uint64) {
	// Mode: bits 0-15
	mode := uint16(perm & 0xFFFF)
	// Group: bits 16-39
	gid := uint32((perm >> 16) & 0xFFFFFF)
	// User: bits 40-63
	uid := uint32((perm >> 40) & 0xFFFFFF)

	record.UserName = ctx.StringTable[uid]
	record.GroupName = ctx.StringTable[gid]
	record.Permission = formatMode(mode, record.Type == "DIRECTORY")
}

func formatMode(mode uint16, isDir bool) string {
	res := make([]byte, 10)
	if isDir {
		res[0] = 'd'
	} else {
		res[0] = '-'
	}

	chars := "rwxrwxrwx"
	for i := 0; i < 9; i++ {
		if mode&(1<<uint(8-i)) != 0 {
			res[i+1] = chars[i]
		} else {
			res[i+1] = '-'
		}
	}

	// Handle Special Bits
	// SetUID (bit 11, octal 4000)
	if mode&04000 != 0 {
		if res[3] == 'x' {
			res[3] = 's'
		} else {
			res[3] = 'S'
		}
	}

	// SetGID (bit 10, octal 2000)
	if mode&02000 != 0 {
		if res[6] == 'x' {
			res[6] = 's'
		} else {
			res[6] = 'S'
		}
	}

	// Sticky Bit (bit 9, octal 1000)
	if mode&01000 != 0 {
		if res[9] == 'x' {
			res[9] = 't'
		} else {
			res[9] = 'T'
		}
	}

	return string(res)
}
