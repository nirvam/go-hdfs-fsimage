package fsimage

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/nirvam/go-hdfs-fsimage/pkg/exporter"
)

type mockReporter struct{}

func (m *mockReporter) Add(int) error { return nil }
func (m *mockReporter) ChangeMax(int) {}

func TestIntegration_RealFSImage(t *testing.T) {
	samplePath := filepath.Join("testdata", "sample.fsimage")
	if _, err := os.Stat(samplePath); os.IsNotExist(err) {
		t.Skip("Sample fsimage not found, skipping integration test")
	}

	img, err := NewFSImage(samplePath)
	if err != nil {
		t.Fatalf("Failed to open sample fsimage: %v", err)
	}
	defer img.Close()

	ctx := NewPass1Context()
	// Pass 1
	if err := img.RunPass1(ctx, io.Discard); err != nil {
		t.Fatalf("Pass 1 failed on real fsimage: %v", err)
	}

	// Pass 2 with null exporter to verify core logic
	exp := &nullExporter{}
	if err := img.RunPass2(ctx, exp, &mockReporter{}); err != nil {
		t.Fatalf("Pass 2 failed on real fsimage: %v", err)
	}

	if len(ctx.IDToName) == 0 {
		t.Error("Pass 1 found 0 directories in real fsimage, which is unlikely")
	}
	t.Logf("Found %d directories and %d total children in ChildToParent", len(ctx.IDToName), len(ctx.ChildToParent))
}

func BenchmarkRealFSImageProcessing(b *testing.B) {
	samplePath := filepath.Join("testdata", "sample.fsimage")
	if _, err := os.Stat(samplePath); os.IsNotExist(err) {
		b.Skip("Sample fsimage not found")
	}

	for i := 0; i < b.N; i++ {
		img, _ := NewFSImage(samplePath)
		ctx := NewPass1Context()
		_ = img.RunPass1(ctx, io.Discard)
		exp := &nullExporter{}
		_ = img.RunPass2(ctx, exp, &mockReporter{})
		img.Close()
	}
}

type nullExporter struct{}

func (e *nullExporter) Export(record *exporter.INodeRecord) error       { return nil }
func (e *nullExporter) ExportStringTable(table map[uint32]string) error { return nil }
func (e *nullExporter) Close() error                                    { return nil }
