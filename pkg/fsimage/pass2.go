package fsimage

import (
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/nirvam/go-hdfs-fsimage/pkg/exporter"
	"github.com/nirvam/go-hdfs-fsimage/pkg/fsimage/types"
	"google.golang.org/protobuf/proto"
)

const (
	RootInodeID = 16385
)

type ProgressReporter interface {
	Add(int) error
	ChangeMax(int)
}

type inodeJob struct {
	data []byte
}

func (img *FSImage) RunPass2(ctx *Pass1Context, exp exporter.Exporter, bar ProgressReporter) error {
	r, err := img.OpenSection("INODE")
	if err != nil {
		return err
	}
	defer r.Close()

	// Pre-allocate Cache based on number of directories found in Pass 1 to avoid rehashing
	if len(ctx.IDToName) > 0 {
		ctx.mu.Lock()
		ctx.DirPathCache = make(map[uint64]string, len(ctx.IDToName))
		ctx.mu.Unlock()
	}

	// Read header
	header := &types.INodeSection{}
	if err := ReadDelimited(r, header); err != nil {
		return err
	}

	numINodes := header.GetNumInodes()
	if bar != nil {
		bar.ChangeMax(int(numINodes))
	}

	numWorkers := runtime.NumCPU()
	jobChan := make(chan *inodeJob, numWorkers*16)
	resultChan := make(chan *exporter.INodeRecord, numWorkers*16)
	errChan := make(chan error, numWorkers+2)

	var wg sync.WaitGroup

	// 1. Workers: Parallel Processing
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localInode := &types.INodeSection_INode{}
			for job := range jobChan {
				localInode.Reset()
				if err := proto.Unmarshal(job.data, localInode); err != nil {
					errChan <- err
					BytePool.Put(job.data)
					return
				}
				BytePool.Put(job.data)

				record := exporter.InodeRecordPool.Get().(*exporter.INodeRecord)
				record.Reset()
				img.fillINodeRecord(ctx, record, localInode)
				resultChan <- record
			}
		}()
	}

	// 2. Exporter: Single-threaded Consumer (Required for most database appenders)
	exportDone := make(chan struct{})
	go func() {
		defer close(exportDone)
		var i uint64
		for record := range resultChan {
			if err := exp.Export(record); err != nil {
				errChan <- err
				exporter.InodeRecordPool.Put(record)
				return
			}
			exporter.InodeRecordPool.Put(record)
			i++
			if bar != nil && i%10000 == 0 {
				bar.Add(10000)
			}
			if i == numINodes {
				if bar != nil && numINodes%10000 != 0 {
					bar.Add(int(numINodes % 10000))
				}
			}
		}
	}()

	// 3. Producer: Read and dispatch
	var produceErr error
	for i := uint64(0); i < numINodes; i++ {
		length, err := ReadDelimitedHeader(r)
		if err != nil {
			produceErr = err
			break
		}

		data := BytePool.Get().([]byte)
		if uint64(len(data)) < length {
			data = make([]byte, length)
		}
		if _, err := io.ReadFull(r, data[:length]); err != nil {
			BytePool.Put(data)
			produceErr = err
			break
		}

		select {
		case err := <-errChan:
			return err
		case jobChan <- &inodeJob{data: data[:length]}:
		}
	}

	close(jobChan)
	wg.Wait()
	close(resultChan)
	<-exportDone

	if produceErr != nil {
		return produceErr
	}

	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

func (img *FSImage) fillINodeRecord(ctx *Pass1Context, record *exporter.INodeRecord, inode *types.INodeSection_INode) {
	id := inode.GetId()
	var name string
	if inode.GetType() == types.INodeSection_INode_DIRECTORY {
		name = ctx.IDToName[id]
	} else {
		name = string(inode.GetName())
	}

	record.ID = id
	record.RawType = uint8(inode.GetType())
	record.Type = inode.GetType().String()

	// Calculate Path using parent cache
	if id == RootInodeID {
		record.Path = "/"
	} else {
		parentID := ctx.ChildToParent[id]
		parentPath := img.getDirPath(ctx, parentID)
		// For children of root, parentPath will be "", so we get "/name"
		record.Path = parentPath + "/" + name
	}

	// Cache directory path for future children
	if inode.GetType() == types.INodeSection_INode_DIRECTORY {
		ctx.mu.Lock()
		ctx.DirPathCache[id] = record.Path
		ctx.mu.Unlock()
	}

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

func (img *FSImage) getDirPath(ctx *Pass1Context, id uint64) string {
	if id == RootInodeID {
		return "" // Returns empty so child will be "" + "/" + name = "/name"
	}

	ctx.mu.RLock()
	p, ok := ctx.DirPathCache[id]
	ctx.mu.RUnlock()
	if ok {
		return p
	}

	// If not cached, we need to build it recursively (rare, as children usually come after parents)
	parentID, ok := ctx.ChildToParent[id]
	if !ok {
		return ""
	}

	name := ctx.IDToName[id]
	parentPath := img.getDirPath(ctx, parentID)
	fullPath := parentPath + "/" + name

	ctx.mu.Lock()
	ctx.DirPathCache[id] = fullPath
	ctx.mu.Unlock()

	return fullPath
}

func (img *FSImage) fillPermission(ctx *Pass1Context, record *exporter.INodeRecord, perm uint64) {
	// Mode: bits 0-15
	mode := uint16(perm & 0xFFFF)
	// Group: bits 16-39
	gid := uint32((perm >> 16) & 0xFFFFFF)
	// User: bits 40-63
	uid := uint32((perm >> 40) & 0xFFFFFF)

	record.UserID = uid
	record.GroupID = gid
	record.RawPermission = mode

	record.UserName = ctx.StringTable[uid]
	record.GroupName = ctx.StringTable[gid]
	record.Permission = formatMode(mode, record.RawType == uint8(types.INodeSection_INode_DIRECTORY))
}

func formatMode(mode uint16, isDir bool) string {
	// Standardize common patterns to avoid allocation
	if !isDir {
		switch mode & 0777 {
		case 0644:
			return "-rw-r--r--"
		case 0755:
			return "-rwxr-xr-x"
		case 0600:
			return "-rw-------"
		}
	} else {
		switch mode & 0777 {
		case 0755:
			return "drwxr-xr-x"
		case 0700:
			return "drwx------"
		case 0775:
			return "drwxrwxr-x"
		}
	}

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
