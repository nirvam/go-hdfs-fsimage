package fsimage

import (
	"fmt"
	"io"

	"github.com/nirvam/go-hdfs-fsimage/pkg/fsimage/types"
)

type Pass1Context struct {
	StringTable   map[uint32]string
	IDToName      map[uint64]string
	ChildToParent map[uint64]uint64
	RefList       []uint64 // Index is the position in INodeReferenceSection
}

func NewPass1Context() *Pass1Context {
	return &Pass1Context{
		StringTable:   make(map[uint32]string),
		IDToName:      make(map[uint64]string),
		ChildToParent: make(map[uint64]uint64),
		RefList:       make([]uint64, 0),
	}
}

func (img *FSImage) RunPass1(ctx *Pass1Context) error {
	// 1. Load String Table
	if err := img.loadStringTable(ctx); err != nil {
		return fmt.Errorf("failed to load string table: %w", err)
	}

	// 2. Load INode Reference Section (Must be before INODE_DIR)
	if err := img.loadINodeReferenceSection(ctx); err != nil {
		return fmt.Errorf("failed to load inode reference section: %w", err)
	}

	// 3. Load INodes (id -> name)
	if err := img.loadINodeSection(ctx); err != nil {
		return fmt.Errorf("failed to load inode section: %w", err)
	}

	// 4. Load INode Directories (child -> parent)
	if err := img.loadINodeDirSection(ctx); err != nil {
		return fmt.Errorf("failed to load inode dir section: %w", err)
	}

	return nil
}

func (img *FSImage) loadINodeReferenceSection(ctx *Pass1Context) error {
	r, err := img.OpenSection("INODE_REFERENCE")
	if err != nil {
		// This section is optional
		return nil
	}
	defer r.Close()

	entry := &types.INodeReferenceSection_INodeReference{}
	for {
		entry.Reset()
		err := ReadDelimited(r, entry)
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

func (img *FSImage) loadStringTable(ctx *Pass1Context) error {
	r, err := img.OpenSection("STRING_TABLE")
	if err != nil {
		// String table is optional in some versions? No, usually required for Modern PB format.
		return err
	}
	defer r.Close()

	// Read summary
	stHeader := &types.StringTableSection{}
	if err := ReadDelimited(r, stHeader); err != nil {
		return err
	}

	numEntries := stHeader.GetNumEntry()
	entry := &types.StringTableSection_Entry{}
	for i := uint32(0); i < numEntries; i++ {
		entry.Reset()
		if err := ReadDelimited(r, entry); err != nil {
			return err
		}
		ctx.StringTable[entry.GetId()] = entry.GetStr()
	}

	return nil
}

func (img *FSImage) loadINodeSection(ctx *Pass1Context) error {
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
		// During Pass 1, we only need directory names for path backtracking.
		// File names will be read from the stream during Pass 2.
		if inode.GetType() == types.INodeSection_INode_DIRECTORY {
			ctx.IDToName[inode.GetId()] = string(inode.GetName())
		}
	}

	return nil
}

func (img *FSImage) loadINodeDirSection(ctx *Pass1Context) error {
	r, err := img.OpenSection("INODE_DIR")
	if err != nil {
		// This section is optional if there are no directories
		return nil
	}
	defer r.Close()

	entry := &types.INodeDirectorySection_DirEntry{}
	for {
		entry.Reset()
		err := ReadDelimited(r, entry)
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
