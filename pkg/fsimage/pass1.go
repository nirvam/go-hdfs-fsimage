package fsimage

import (
	"fmt"
	"io"
	"sync"

	"github.com/nirvam/go-hdfs-fsimage/pkg/fsimage/types"
	"google.golang.org/protobuf/proto"
)

type Pass1Context struct {
	StringTable   map[uint32]string
	IDToName      map[uint64]string
	ChildToParent map[uint64]uint64
	RefList       []uint64 // Index is the position in INodeReferenceSection
	DirPathCache  map[uint64]string
	mu            sync.RWMutex
}

func NewPass1Context() *Pass1Context {
	return &Pass1Context{
		StringTable:   make(map[uint32]string),
		IDToName:      make(map[uint64]string),
		ChildToParent: make(map[uint64]uint64),
		RefList:       make([]uint64, 0),
		DirPathCache:  make(map[uint64]string),
	}
}

func (img *FSImage) RunPass1(ctx *Pass1Context, bar io.Writer) error {
	// 1. Load String Table
	if err := img.loadStringTable(ctx, bar); err != nil {
		return fmt.Errorf("failed to load string table: %w", err)
	}

	// 2. Load INode Reference Section (Must be before INODE_DIR)
	if err := img.loadINodeReferenceSection(ctx, bar); err != nil {
		return fmt.Errorf("failed to load inode reference section: %w", err)
	}

	// 3. Load INodes (id -> name)
	if err := img.loadINodeSection(ctx, bar); err != nil {
		return fmt.Errorf("failed to load inode section: %w", err)
	}

	// 4. Load INode Directories (child -> parent)
	if err := img.loadINodeDirSection(ctx, bar); err != nil {
		return fmt.Errorf("failed to load inode dir section: %w", err)
	}

	return nil
}

func (img *FSImage) loadINodeReferenceSection(ctx *Pass1Context, bar io.Writer) error {
	r, err := img.OpenSection("INODE_REFERENCE")
	if err != nil {
		// This section is optional
		return nil
	}
	defer r.Close()

	// Wrap reader to track progress
	tr := io.TeeReader(r, bar)

	entry := &types.INodeReferenceSection_INodeReference{}
	for {
		entry.Reset()
		err := ReadDelimited(tr, entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		ctx.RefList = append(ctx.RefList, entry.GetReferredId())
	}
	return nil
}

func (img *FSImage) loadStringTable(ctx *Pass1Context, bar io.Writer) error {
	r, err := img.OpenSection("STRING_TABLE")
	if err != nil {
		// String table is optional in some versions? No, usually required for Modern PB format.
		return err
	}
	defer r.Close()

	tr := io.TeeReader(r, bar)

	// Read summary
	stHeader := &types.StringTableSection{}
	if err := ReadDelimited(tr, stHeader); err != nil {
		return err
	}

	numEntries := stHeader.GetNumEntry()
	entry := &types.StringTableSection_Entry{}
	for i := uint32(0); i < numEntries; i++ {
		entry.Reset()
		if err := ReadDelimited(tr, entry); err != nil {
			return err
		}
		ctx.StringTable[entry.GetId()] = entry.GetStr()
	}

	return nil
}

func (img *FSImage) loadINodeSection(ctx *Pass1Context, bar io.Writer) error {
	r, err := img.OpenSection("INODE")
	if err != nil {
		return err
	}
	defer r.Close()

	tr := io.TeeReader(r, bar)

	// Read header
	header := &types.INodeSection{}
	if err := ReadDelimited(tr, header); err != nil {
		return err
	}

	numINodes := header.GetNumInodes()
	inode := &types.INodeSection_INode{}

	// We can't use type assertion on tr because it's an io.TeeReader.
	// We need to use r (which is the buffered reader) for peeking.
	peeker, ok := r.(interface {
		Peek(int) ([]byte, error)
	})

	for i := uint64(0); i < numINodes; i++ {
		length, err := ReadDelimitedHeader(tr)
		if err != nil {
			return err
		}

		shouldSkip := false
		if ok {
			peek, err := peeker.Peek(2)
			if err == nil && len(peek) >= 2 {
				// Protobuf Tag 0x08 = Field 1 (Type), Type 0 (Varint)
				if peek[0] == 0x08 {
					if peek[1] != 0x02 { // Not a DIRECTORY (Value 2)
						shouldSkip = true
					}
				}
			}
		}

		if !shouldSkip {
			// Read and unmarshal using pool
			data := BytePool.Get().([]byte)
			if uint64(len(data)) < length {
				data = make([]byte, length)
			}
			if _, err := io.ReadFull(tr, data[:length]); err != nil {
				BytePool.Put(data)
				return err
			}
			inode.Reset()
			if err := proto.Unmarshal(data[:length], inode); err != nil {
				BytePool.Put(data)
				return err
			}
			if inode.GetType() == types.INodeSection_INode_DIRECTORY {
				ctx.IDToName[inode.GetId()] = string(inode.GetName())
			}
			BytePool.Put(data)
		} else {
			// Skip and just update progress bar
			if _, err := io.CopyN(io.Discard, tr, int64(length)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (img *FSImage) loadINodeDirSection(ctx *Pass1Context, bar io.Writer) error {
	r, err := img.OpenSection("INODE_DIR")
	if err != nil {
		// This section is optional if there are no directories
		return nil
	}
	defer r.Close()

	tr := io.TeeReader(r, bar)

	entry := &types.INodeDirectorySection_DirEntry{}
	for {
		entry.Reset()
		err := ReadDelimited(tr, entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		parentID := entry.GetParent()
		for _, childID := range entry.GetChildren() {
			ctx.ChildToParent[childID] = parentID
		}

		// Handle reference children (Snapshot refs)
		for _, refIdx := range entry.GetRefChildren() {
			if int(refIdx) < len(ctx.RefList) {
				childID := ctx.RefList[refIdx]
				ctx.ChildToParent[childID] = parentID
			}
		}
	}

	return nil
}
