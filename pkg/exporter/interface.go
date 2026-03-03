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
	UserName           string
	GroupName          string
	Type               string // FILE, DIRECTORY, SYMLINK
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
	r.UserName = ""
	r.GroupName = ""
	r.Type = ""
}

// Exporter is the interface for different output formats.
type Exporter interface {
	Export(record *INodeRecord) error
	Close() error
}
