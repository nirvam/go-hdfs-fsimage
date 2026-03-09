package exporter

import (
	"encoding/csv"
	"os"
	"strconv"
)

type CSVExporter struct {
	file   *os.File
	writer *csv.Writer
}

func NewCSVExporter(path string) (*CSVExporter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(f)

	// Write Header
	header := []string{
		"ID", "Path", "Type", "Replication", "ModificationTime",
		"AccessTime", "PreferredBlockSize", "BlocksCount",
		"FileSize", "Permission", "UserName", "GroupName",
	}
	if err := w.Write(header); err != nil {
		f.Close()
		return nil, err
	}

	return &CSVExporter{file: f, writer: w}, nil
}

func (e *CSVExporter) ExportStringTable(table map[uint32]string) error {
	return nil
}

func (e *CSVExporter) Export(record *INodeRecord) error {
	row := []string{
		strconv.FormatUint(record.ID, 10),
		record.Path,
		record.Type,
		strconv.FormatUint(uint64(record.Replication), 10),
		record.ModificationTime.Format("2006-01-02 15:04:05"),
		record.AccessTime.Format("2006-01-02 15:04:05"),
		strconv.FormatUint(record.PreferredBlockSize, 10),
		strconv.Itoa(record.BlocksCount),
		strconv.FormatUint(record.FileSize, 10),
		record.Permission,
		record.UserName,
		record.GroupName,
	}
	return e.writer.Write(row)
}

func (e *CSVExporter) Close() error {
	e.writer.Flush()
	if err := e.writer.Error(); err != nil {
		return err
	}
	return e.file.Close()
}
