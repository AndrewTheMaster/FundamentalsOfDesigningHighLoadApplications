package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type pgConfig struct {
	DatasetPath string
	DatasetName string
	Delimiter   rune
	RowLimit    int
	DBHost      string
	DBPort      string
	DBUser      string
	DBPassword  string
	DBName      string
}

type pgResult struct {
	Dataset     string        `json:"dataset"`
	Rows        int           `json:"rows"`
	WriteTime   time.Duration `json:"write_time_s"`
	ReadTime    time.Duration `json:"read_time_s"`
	DBSizeMB    float64       `json:"db_size_mb"`
	CSVSizeMB   float64       `json:"csv_size_mb"`
	Compression float64       `json:"compression_ratio"`
}

func main() {
	cfg := parseFlags()

	if err := runBench(cfg); err != nil {
		log.Fatalf("bench failed: %v", err)
	}
}

func parseFlags() pgConfig {
	var (
		datasetPath = flag.String("dataset", "", "path to source CSV file")
		datasetName = flag.String("name", "", "logical dataset name")
		delimiter   = flag.String("delim", ",", "field delimiter")
		rowLimit    = flag.Int("limit", 0, "optional max rows (0 = all)")
		dbHost      = flag.String("db-host", "localhost", "PostgreSQL host")
		dbPort      = flag.String("db-port", "5432", "PostgreSQL port")
		dbUser      = flag.String("db-user", "postgres", "PostgreSQL user")
		dbPassword  = flag.String("db-password", "postgres", "PostgreSQL password")
		dbName      = flag.String("db-name", "benchmark", "PostgreSQL database")
	)

	flag.Parse()

	if *datasetPath == "" {
		log.Fatal("dataset path is required")
	}

	if *datasetName == "" {
		base := filepath.Base(*datasetPath)
		*datasetName = strings.TrimSuffix(base, filepath.Ext(base))
	}

	if len(*delimiter) != 1 {
		log.Fatal("delimiter must be a single rune")
	}

	return pgConfig{
		DatasetPath: *datasetPath,
		DatasetName: *datasetName,
		Delimiter:   ([]rune(*delimiter))[0],
		RowLimit:    *rowLimit,
		DBHost:      *dbHost,
		DBPort:      *dbPort,
		DBUser:      *dbUser,
		DBPassword:  *dbPassword,
		DBName:      *dbName,
	}
}

func runBench(cfg pgConfig) error {
	// Get CSV size
	csvInfo, err := os.Stat(cfg.DatasetPath)
	if err != nil {
		return fmt.Errorf("stat CSV: %w", err)
	}
	csvSizeMB := float64(csvInfo.Size()) / (1024 * 1024)

	// Connect to PostgreSQL
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	tableName := fmt.Sprintf("bench_%s", cfg.DatasetName)

	// Create table
	if _, err := db.Exec(fmt.Sprintf(`
		DROP TABLE IF EXISTS %s CASCADE;
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			key VARCHAR(255) NOT NULL,
			value JSONB NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		);
		CREATE INDEX idx_%s_key ON %s(key);
	`, tableName, tableName, tableName, tableName)); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// Import data
	file, err := os.Open(cfg.DatasetPath)
	if err != nil {
		return fmt.Errorf("open CSV: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = cfg.Delimiter
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	// Skip header if present
	_, _ = reader.Read()

	startWrite := time.Now()
	totalRows := 0
	batchSize := 10000
	batch := make([]string, 0, batchSize)
	stmt, err := db.Prepare(fmt.Sprintf(
		"INSERT INTO %s (key, value) VALUES ($1, $2)", tableName))
	if err != nil {
		return fmt.Errorf("prepare stmt: %w", err)
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read CSV: %w", err)
		}

		sumLen := 0
		for _, field := range record {
			sumLen += len(field)
		}

		payload, err := json.Marshal(struct {
			RowIndex int      `json:"row_index"`
			Fields   []string `json:"fields"`
			SumLen   int      `json:"sum_len"`
		}{
			RowIndex: totalRows,
			Fields:   record,
			SumLen:   sumLen,
		})
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}

		key := fmt.Sprintf("%s:%09d", cfg.DatasetName, totalRows)
		batch = append(batch, key, string(payload))

		if len(batch) >= batchSize*2 {
			if err := insertBatch(db, stmt, batch); err != nil {
				return fmt.Errorf("insert batch: %w", err)
			}
			batch = batch[:0]
		}

		totalRows++
		if cfg.RowLimit > 0 && totalRows >= cfg.RowLimit {
			break
		}

		if totalRows%250000 == 0 {
			log.Printf("[%s] processed %d rows", cfg.DatasetName, totalRows)
		}
	}

	if len(batch) > 0 {
		if err := insertBatch(db, stmt, batch); err != nil {
			return fmt.Errorf("insert batch: %w", err)
		}
	}

	writeTime := time.Since(startWrite)
	stmt.Close()

	// Get database size
	var dbSizeMB float64
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT pg_database_size('%s') / 1024.0 / 1024.0", cfg.DBName)).Scan(&dbSizeMB); err != nil {
		return fmt.Errorf("get db size: %w", err)
	}

	// Full table scan (read benchmark)
	startRead := time.Now()
	var count int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count); err != nil {
		return fmt.Errorf("count: %w", err)
	}
	// Full scan
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		return fmt.Errorf("full scan: %w", err)
	}
	for rows.Next() {
		// Just iterate, don't process
	}
	rows.Close()
	readTime := time.Since(startRead)

	compression := csvSizeMB / dbSizeMB

	result := pgResult{
		Dataset:     cfg.DatasetName,
		Rows:        totalRows,
		WriteTime:   writeTime,
		ReadTime:    readTime,
		DBSizeMB:    dbSizeMB,
		CSVSizeMB:   csvSizeMB,
		Compression: compression,
	}

	// Save result
	if err := os.MkdirAll("bench-results", 0o755); err != nil {
		return fmt.Errorf("create bench-results: %w", err)
	}

	resultJSON, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}

	resultPath := filepath.Join("bench-results", fmt.Sprintf("pg_%s.json", cfg.DatasetName))
	if err := os.WriteFile(resultPath, resultJSON, 0o644); err != nil {
		return fmt.Errorf("write result: %w", err)
	}

	// Print summary
	fmt.Printf("%s\trows=%d\twrite_ms=%d\tread_ms=%d\tdb_mb=%.2f\tcsv_mb=%.2f\tcompression=%.2fx\n",
		result.Dataset,
		result.Rows,
		int(result.WriteTime.Milliseconds()),
		int(result.ReadTime.Milliseconds()),
		result.DBSizeMB,
		result.CSVSizeMB,
		result.Compression,
	)

	return nil
}

func insertBatch(db *sql.DB, stmt *sql.Stmt, batch []string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i := 0; i < len(batch); i += 2 {
		if _, err := tx.Stmt(stmt).Exec(batch[i], batch[i+1]); err != nil {
			return err
		}
	}

	return tx.Commit()
}
