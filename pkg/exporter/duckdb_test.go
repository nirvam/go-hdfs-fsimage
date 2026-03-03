package exporter

import (
	"database/sql"
	"os"
	"testing"
	"time"
)

func TestDuckDBExporter(t *testing.T) {
	dbPath := "test_inodes.duckdb"
	defer os.Remove(dbPath)

	exp, err := NewDuckDBExporter(dbPath)
	if err != nil {
		t.Fatalf("Failed to create exporter: %v", err)
	}

	record := &INodeRecord{
		ID:               16386,
		Path:             "/user",
		Type:             "DIRECTORY",
		Replication:      0,
		ModificationTime: time.Now().Truncate(time.Millisecond),
		AccessTime:       time.Now().Truncate(time.Millisecond),
		Permission:       "drwxr-xr-x",
		UserName:         "hadoop",
		GroupName:        "supergroup",
	}

	if err := exp.Export(record); err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	if err := exp.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify data
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM inodes").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}

	var path string
	err = db.QueryRow("SELECT path FROM inodes WHERE id = 16386").Scan(&path)
	if err != nil {
		t.Fatal(err)
	}

	if path != "/user" {
		t.Errorf("Expected path /user, got %s", path)
	}
}
