package fsimage

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/nirvam/go-hdfs-fsimage/pkg/fsimage/types"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	MagicHeader = "HDFSIMG1"
	FileVersion = 1
)

var (
	// pool for byte buffers to avoid reallocating on every message
	bytePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 1024*64) // 64KB initial buffer
		},
	}
)

type FSImage struct {
	file    *os.File
	Summary *types.FileSummary
}

func NewFSImage(path string) (*FSImage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	img := &FSImage{file: f}
	if err := img.load(); err != nil {
		f.Close()
		return nil, err
	}

	return img, nil
}

func (img *FSImage) Close() error {
	return img.file.Close()
}

func (img *FSImage) load() error {
	// Check Magic
	magic := make([]byte, len(MagicHeader))
	if _, err := img.file.ReadAt(magic, 0); err != nil {
		return fmt.Errorf("failed to read magic: %w", err)
	}
	if string(magic) != MagicHeader {
		return fmt.Errorf("invalid magic header: %s", string(magic))
	}

	// Load Summary
	info, err := img.file.Stat()
	if err != nil {
		return err
	}
	fileSize := info.Size()

	// Last 4 bytes is the summary length
	var summaryLen uint32
	// Use ReadAt with a buffer to read the last 4 bytes
	buf4 := make([]byte, 4)
	if _, err := img.file.ReadAt(buf4, fileSize-4); err != nil {
		return fmt.Errorf("failed to read summary length: %w", err)
	}
	summaryLen = binary.BigEndian.Uint32(buf4)

	// Read summary bytes
	summaryOffset := fileSize - 4 - int64(summaryLen)
	summaryBytes := make([]byte, summaryLen)
	if _, err := img.file.ReadAt(summaryBytes, summaryOffset); err != nil {
		return fmt.Errorf("failed to read summary: %w", err)
	}

	// Parse delimited summary
	summary := &types.FileSummary{}
	if err := UnmarshalDelimited(summaryBytes, summary); err != nil {
		return fmt.Errorf("failed to parse summary: %w", err)
	}

	if summary.GetOndiskVersion() != FileVersion {
		return fmt.Errorf("unsupported on-disk version: %d", summary.GetOndiskVersion())
	}

	img.Summary = summary
	return nil
}

// OpenSection returns a reader for the named section.
func (img *FSImage) OpenSection(name string) (io.ReadCloser, error) {
	var targetSection *types.FileSummary_Section
	for _, s := range img.Summary.GetSections() {
		if s.GetName() == name {
			targetSection = s
			break
		}
	}

	if targetSection == nil {
		return nil, fmt.Errorf("section not found: %s", name)
	}

	sectionReader := io.NewSectionReader(img.file, int64(targetSection.GetOffset()), int64(targetSection.GetLength()))

	var r io.ReadCloser
	codec := img.Summary.GetCodec()
	if codec == "" {
		r = io.NopCloser(sectionReader)
	} else {
		// Handle compression
		switch codec {
		case "org.apache.hadoop.io.compress.GzipCodec":
			gzipReader, err := gzip.NewReader(sectionReader)
			if err != nil {
				return nil, err
			}
			r = gzipReader
		default:
			return nil, fmt.Errorf("unsupported compression codec: %s", codec)
		}
	}

	// Always wrap in a buffered reader for efficient Varint reading
	return struct {
		*bufio.Reader
		io.Closer
	}{
		Reader: bufio.NewReader(r),
		Closer: r,
	}, nil
}

// UnmarshalDelimited parses a delimited protobuf message from a byte slice.
func UnmarshalDelimited(data []byte, msg proto.Message) error {
	length, n := protowire.ConsumeVarint(data)
	if n <= 0 {
		return fmt.Errorf("failed to consume varint: %d", n)
	}
	if uint64(len(data)-n) < length {
		return fmt.Errorf("insufficient data for delimited message: expected %d, got %d", length, len(data)-n)
	}
	return proto.Unmarshal(data[n:n+int(length)], msg)
}

// ReadDelimited reads a delimited protobuf message from an io.Reader.
func ReadDelimited(r io.Reader, msg proto.Message) error {
	// 1. Read the varint length prefix
	var buf [binary.MaxVarintLen64]byte
	var length uint64
	var n int

	// Optimization: If r is a *bufio.Reader, we can use Peek
	if br, ok := r.(*bufio.Reader); ok {
		peek, err := br.Peek(binary.MaxVarintLen64)
		if err != nil && err != io.EOF && len(peek) == 0 {
			return err
		}
		var m int
		length, m = protowire.ConsumeVarint(peek)
		if m > 0 {
			n = m
			br.Discard(m)
		}
	}

	if n == 0 {
		// Fallback for non-buffered reader
		for i := 0; i < binary.MaxVarintLen64; i++ {
			if _, err := r.Read(buf[i : i+1]); err != nil {
				return err
			}
			var m int
			length, m = protowire.ConsumeVarint(buf[:i+1])
			if m > 0 {
				n = m
				break
			}
		}
	}

	if n == 0 {
		return fmt.Errorf("failed to read varint")
	}

	// 2. Read the message body using a pooled buffer
	dataPtr := bytePool.Get().([]byte)
	if uint64(len(dataPtr)) < length {
		dataPtr = make([]byte, length)
	}
	defer bytePool.Put(dataPtr)

	if _, err := io.ReadFull(r, dataPtr[:length]); err != nil {
		return err
	}

	return proto.Unmarshal(dataPtr[:length], msg)
}
