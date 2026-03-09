package exporter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
)

type DuckDBExporter struct {
	db       *sql.DB
	conn     driver.Conn
	appender *duckdb.Appender
}

func NewDuckDBExporter(path string) (*DuckDBExporter, error) {
	connector, err := duckdb.NewConnector(path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create duckdb connector: %w", err)
	}

	con, err := connector.Connect(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	db := sql.OpenDB(connector)
	// Create table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS inodes (
			id UBIGINT,
			path VARCHAR,
			type UTINYINT,
			replication UINTEGER,
			modification_time TIMESTAMP,
			access_time TIMESTAMP,
			preferred_block_size UBIGINT,
			blocks_count INTEGER,
			file_size UBIGINT,
			permission USMALLINT,
			user_id UINTEGER,
			group_id UINTEGER
		)
	`)
	if err != nil {
		con.Close()
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	appender, err := duckdb.NewAppenderFromConn(con, "", "inodes")
	if err != nil {
		con.Close()
		db.Close()
		return nil, fmt.Errorf("failed to create appender: %w", err)
	}

	return &DuckDBExporter{db: db, conn: con, appender: appender}, nil
}

func (e *DuckDBExporter) ExportStringTable(table map[uint32]string) error {
	_, err := e.db.Exec(`CREATE TABLE IF NOT EXISTS string_table (id UINTEGER, value VARCHAR)`)
	if err != nil {
		return err
	}

	appender, err := duckdb.NewAppenderFromConn(e.conn, "", "string_table")
	if err != nil {
		return err
	}
	defer appender.Close()

	for id, val := range table {
		if err := appender.AppendRow(id, val); err != nil {
			return err
		}
	}
	return nil
}

func (e *DuckDBExporter) Export(record *INodeRecord) error {
	return e.appender.AppendRow(
		record.ID,
		record.Path,
		record.RawType,
		record.Replication,
		record.ModificationTime,
		record.AccessTime,
		record.PreferredBlockSize,
		int32(record.BlocksCount),
		record.FileSize,
		record.RawPermission,
		record.UserID,
		record.GroupID,
	)
}

func (e *DuckDBExporter) Close() error {
	if err := e.appender.Close(); err != nil {
		return err
	}
	if err := e.conn.Close(); err != nil {
		return err
	}
	return e.db.Close()
}
