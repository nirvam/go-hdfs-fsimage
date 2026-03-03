package fsimage

import (
	"bytes"
	"testing"

	"github.com/nirvam/go-hdfs-fsimage/pkg/fsimage/types"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestReadDelimited(t *testing.T) {
	// 1. Create a dummy PB message
	msg := &types.StringTableSection_Entry{
		Id:  proto.Uint32(1),
		Str: proto.String("test-string"),
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Wrap with Varint length
	var buf bytes.Buffer
	n := protowire.AppendVarint(nil, uint64(len(data)))
	buf.Write(n)
	buf.Write(data)

	// 3. Test ReadDelimited
	got := &types.StringTableSection_Entry{}
	if err := ReadDelimited(&buf, got); err != nil {
		t.Errorf("ReadDelimited failed: %v", err)
	}

	if got.GetId() != 1 || got.GetStr() != "test-string" {
		t.Errorf("ReadDelimited mismatch: got %v, want %v", got, msg)
	}
}

func TestUnmarshalDelimited(t *testing.T) {
	msg := &types.FileSummary{
		OndiskVersion: proto.Uint32(1),
		LayoutVersion: proto.Uint32(32),
	}
	data, _ := proto.Marshal(msg)

	n := protowire.AppendVarint(nil, uint64(len(data)))
	fullData := append(n, data...)

	got := &types.FileSummary{}
	if err := UnmarshalDelimited(fullData, got); err != nil {
		t.Errorf("UnmarshalDelimited failed: %v", err)
	}

	if got.GetOndiskVersion() != 1 || got.GetLayoutVersion() != 32 {
		t.Errorf("UnmarshalDelimited mismatch")
	}
}
