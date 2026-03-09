package exporter

import (
	"sync"
	"time"
)

// INodeRecord represents a single INode with its full metadata and path.
type INodeRecord struct {
	ID                 uint64
	Path               string
	Replication        uint32
	ModificationTime   time.Time
	AccessTime         time.Time
	PreferredBlockSize uint64
	BlocksCount        int
	FileSize           uint64
	Permission         string // rwxr-xr-x
	RawPermission      uint16
	UserName           string
	UserID             uint32
	GroupName          string
	GroupID            uint32
	Type               string // FILE, DIRECTORY, SYMLINK
	RawType            uint8  // 1=FILE, 2=DIRECTORY, 3=SYMLINK
}

var (
	InodeRecordPool = sync.Pool{
		New: func() interface{} {
			return &INodeRecord{}
		},
	}
)

func (r *INodeRecord) Reset() {
	r.ID = 0
	r.Path = ""
	r.Replication = 0
	r.ModificationTime = time.Time{}
	r.AccessTime = time.Time{}
	r.PreferredBlockSize = 0
	r.BlocksCount = 0
	r.FileSize = 0
	r.Permission = ""
	r.RawPermission = 0
	r.UserName = ""
	r.UserID = 0
	r.GroupName = ""
	r.GroupID = 0
	r.Type = ""
	r.RawType = 0
}

// Exporter is the interface for different output formats.
type Exporter interface {
	Export(record *INodeRecord) error
	ExportStringTable(table map[uint32]string) error
	Close() error
}
