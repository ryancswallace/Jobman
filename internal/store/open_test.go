package store

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOpenInitializesPrivateSQLite(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state")
	fixedTime := time.Date(2026, time.July, 14, 12, 30, 0, 123, time.UTC)
	store, err := Open(t.Context(), Options{
		StateDir:      stateDir,
		BusyTimeout:   250 * time.Millisecond,
		JobmanVersion: "test-version",
		Now:           func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})

	if !filepath.IsAbs(store.StateDir()) {
		t.Errorf("StateDir() = %q, want absolute path", store.StateDir())
	}
	if got, want := store.DatabasePath(), filepath.Join(store.StateDir(), DatabaseFilename); got != want {
		t.Errorf("DatabasePath() = %q, want %q", got, want)
	}
	if major, minor, patch, parseErr := parseSQLiteVersion(store.SQLiteVersion()); parseErr != nil {
		t.Errorf("parseSQLiteVersion(SQLiteVersion()) error = %v", parseErr)
	} else if versionLessThan(major, minor, patch, minimumSQLiteMajor, minimumSQLiteMinor, minimumSQLitePatch) {
		t.Errorf("SQLiteVersion() = %q, want at least 3.51.3", store.SQLiteVersion())
	}

	assertPragmaInt(t, store.db, "application_id", applicationID)
	assertPragmaInt(t, store.db, "user_version", currentSchemaVersion)
	assertPragmaInt(t, store.db, "foreign_keys", 1)
	assertPragmaInt(t, store.db, "synchronous", 2)
	assertPragmaInt(t, store.db, "busy_timeout", 250)
	assertPragmaText(t, store.db, "journal_mode", "wal")

	wantTables := []string{
		"admission_requests",
		"admissions",
		"concurrency_limits",
		"job_dependencies",
		"job_runtime",
		"job_tags",
		"jobs",
		"notification_attempts",
		"notification_deliveries",
		"run_diagnostic_facts",
		"run_log_pruning",
		"runs",
		"schema_migrations",
		"state_events",
		"store_secrets",
		"supervisors",
		"wait_evaluations",
	}
	rows, err := store.db.QueryContext(t.Context(), `
		SELECT name
		FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		ORDER BY name`)
	if err != nil {
		t.Fatalf("query schema tables: %v", err)
	}
	defer rows.Close()

	var gotTables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan schema table: %v", err)
		}
		gotTables = append(gotTables, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate schema tables: %v", err)
	}
	if strings.Join(gotTables, ",") != strings.Join(wantTables, ",") {
		t.Errorf("schema tables = %v, want %v", gotTables, wantTables)
	}

	var appliedAt int64
	var version string
	var checksum string
	if err := store.db.QueryRowContext(t.Context(), `
		SELECT applied_at_ns, jobman_version, checksum
		FROM schema_migrations
		WHERE version = 1`).Scan(&appliedAt, &version, &checksum); err != nil {
		t.Fatalf("query migration record: %v", err)
	}
	if appliedAt != fixedTime.UnixNano() {
		t.Errorf("migration applied_at_ns = %d, want %d", appliedAt, fixedTime.UnixNano())
	}
	if version != "test-version" {
		t.Errorf("migration jobman_version = %q, want test-version", version)
	}
	if checksum != migrationChecksum(migration1SQL) {
		t.Errorf("migration checksum = %q, want %q", checksum, migrationChecksum(migration1SQL))
	}
	if err := store.db.QueryRowContext(t.Context(), `
		SELECT checksum FROM schema_migrations WHERE version = 2`).Scan(&checksum); err != nil {
		t.Fatalf("query second migration record: %v", err)
	}
	if checksum != migrationChecksum(migration2SQL) {
		t.Errorf("migration 2 checksum = %q, want %q", checksum, migrationChecksum(migration2SQL))
	}

	if runtime.GOOS != "windows" {
		assertMode(t, store.StateDir(), 0o700)
		assertMode(t, store.DatabasePath(), 0o600)
	}
}

func TestOpenExistingDatabase(t *testing.T) {
	t.Parallel()

	options := Options{StateDir: filepath.Join(t.TempDir(), "state")}
	first, err := Open(t.Context(), options)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if closeErr := first.Close(); closeErr != nil {
		t.Fatalf("first Close() error = %v", closeErr)
	}

	second, secondErr := Open(t.Context(), options)
	if secondErr != nil {
		t.Fatalf("second Open() error = %v", secondErr)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestOpenInitializesRecoversAndPreservesFingerprintKey(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state")
	firstEntropy := bytes.Repeat([]byte{0x71}, fingerprintKeyBytes)
	first, err := Open(t.Context(), Options{StateDir: stateDir, Random: bytes.NewReader(firstEntropy)})
	if err != nil {
		t.Fatalf("Open(first) error = %v", err)
	}
	if !bytes.Equal(first.fingerprintKey[:], firstEntropy) {
		t.Fatal("initial fingerprint key does not match injected entropy")
	}
	if closeErr := first.Close(); closeErr != nil {
		t.Fatalf("Close(first) error = %v", closeErr)
	}

	// Existing current stores need no randomness and therefore no writer merely
	// to load their key.
	second, err := Open(t.Context(), Options{StateDir: stateDir, Random: bytes.NewReader(nil)})
	if err != nil {
		t.Fatalf("Open(existing) error = %v", err)
	}
	if !bytes.Equal(second.fingerprintKey[:], firstEntropy) {
		t.Fatal("reopened store changed its fingerprint key")
	}
	if _, deleteErr := second.db.ExecContext(t.Context(), `DELETE FROM store_secrets`); deleteErr != nil {
		t.Fatalf("simulate interrupted key initialization: %v", deleteErr)
	}
	if closeErr := second.Close(); closeErr != nil {
		t.Fatalf("Close(second) error = %v", closeErr)
	}

	recoveryEntropy := bytes.Repeat([]byte{0x72}, fingerprintKeyBytes)
	recovered, err := Open(t.Context(), Options{StateDir: stateDir, Random: bytes.NewReader(recoveryEntropy)})
	if err != nil {
		t.Fatalf("Open(missing key) error = %v", err)
	}
	defer func() {
		if closeErr := recovered.Close(); closeErr != nil {
			t.Errorf("Close(recovered) error = %v", closeErr)
		}
	}()
	if !bytes.Equal(recovered.fingerprintKey[:], recoveryEntropy) {
		t.Fatal("missing-key recovery did not persist injected entropy")
	}
}

func TestOpenRejectsCorruptFingerprintKey(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state")
	database, err := Open(t.Context(), Options{StateDir: stateDir})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, pragmaErr := database.db.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); pragmaErr != nil {
		t.Fatalf("disable check constraints: %v", pragmaErr)
	}
	if _, updateErr := database.db.ExecContext(t.Context(), `
		UPDATE store_secrets SET value = zeroblob(31) WHERE name = ?`, fingerprintSecretName); updateErr != nil {
		t.Fatalf("corrupt fingerprint key: %v", updateErr)
	}
	if closeErr := database.Close(); closeErr != nil {
		t.Fatalf("Close() error = %v", closeErr)
	}

	opened, err := Open(t.Context(), Options{StateDir: stateDir})
	if opened != nil {
		_ = opened.Close()
	}
	var schemaErr *SchemaError
	if !errors.As(err, &schemaErr) {
		t.Fatalf("Open(corrupt key) error = %v, want *SchemaError", err)
	}
}

func TestOpenRejectsMissingKeyWhenFingerprintsExist(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state")
	database, err := Open(t.Context(), Options{
		StateDir: stateDir, EventIDs: newSequentialEventIDs(0x78a0),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	completeFactFailure(t, database, 0x78b0, "/test/bin/worker", storeTestTime())
	if _, deleteErr := database.db.ExecContext(t.Context(), `DELETE FROM store_secrets`); deleteErr != nil {
		t.Fatalf("remove key: %v", deleteErr)
	}
	if closeErr := database.Close(); closeErr != nil {
		t.Fatalf("Close() error = %v", closeErr)
	}

	opened, err := Open(t.Context(), Options{
		StateDir: stateDir, Random: bytes.NewReader(bytes.Repeat([]byte{0x79}, fingerprintKeyBytes)),
	})
	if opened != nil {
		_ = opened.Close()
	}
	var schemaErr *SchemaError
	if !errors.As(err, &schemaErr) || !strings.Contains(schemaErr.Error(), "indexed fingerprints exist") {
		t.Fatalf("Open(missing used key) error = %v, want fingerprint schema error", err)
	}
}

func TestOpenCurrentSchemaDoesNotRequireWriterLock(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state")
	first, err := Open(t.Context(), Options{StateDir: stateDir})
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := first.Close(); closeErr != nil {
			t.Errorf("first Close() error = %v", closeErr)
		}
	})

	blocking, err := first.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin blocking transaction: %v", err)
	}
	t.Cleanup(func() {
		if rollbackErr := blocking.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			t.Errorf("rollback blocking transaction: %v", rollbackErr)
		}
	})

	second, err := Open(t.Context(), Options{
		StateDir:    stateDir,
		BusyTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open(current schema during write) error = %v", err)
	}
	if closeErr := second.Close(); closeErr != nil {
		t.Fatalf("second Close() error = %v", closeErr)
	}
}

func TestOpenRejectsUnsafePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows access is validated through ACLs")
	}

	stateDir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	// #nosec G302 -- unsafe permissions are intentional rejection input.
	if err := os.Chmod(stateDir, 0o755); err != nil {
		t.Fatalf("Chmod() error = %v", err)
	}

	_, err := Open(t.Context(), Options{StateDir: stateDir})
	if err == nil || !strings.Contains(err.Error(), "unsafe permissions") {
		t.Fatalf("Open() error = %v, want unsafe permissions error", err)
	}
}

func TestOpenRejectsForeignApplicationID(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state")
	path := prepareRawDatabase(t, stateDir)
	raw := openRawDatabase(t, path)
	if _, err := raw.ExecContext(t.Context(), "PRAGMA application_id = 1234"); err != nil {
		t.Fatalf("set foreign application ID: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw database: %v", err)
	}

	_, err := Open(t.Context(), Options{StateDir: stateDir})
	var schemaErr *SchemaError
	if !errors.As(err, &schemaErr) {
		t.Fatalf("Open() error = %v, want *SchemaError", err)
	}
}

func TestOpenRejectsNewerSchema(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state")
	path := prepareRawDatabase(t, stateDir)
	raw := openRawDatabase(t, path)
	if _, err := raw.ExecContext(t.Context(), "PRAGMA application_id = 1246716493"); err != nil {
		t.Fatalf("set application ID: %v", err)
	}
	if _, err := raw.ExecContext(t.Context(), fmt.Sprintf("PRAGMA user_version = %d", currentSchemaVersion+1)); err != nil {
		t.Fatalf("set schema version: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw database: %v", err)
	}

	_, err := Open(t.Context(), Options{StateDir: stateDir})
	var schemaErr *SchemaError
	if !errors.As(err, &schemaErr) {
		t.Fatalf("Open() error = %v, want *SchemaError", err)
	}
}

func TestOpenRejectsMigrationChecksumMismatch(t *testing.T) {
	t.Parallel()

	options := Options{StateDir: filepath.Join(t.TempDir(), "state")}
	store, err := Open(t.Context(), options)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, updateErr := store.db.ExecContext(t.Context(), `
		UPDATE schema_migrations
		SET checksum = ?
		WHERE version = 1`, strings.Repeat("0", 64)); updateErr != nil {
		t.Fatalf("corrupt migration checksum: %v", updateErr)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatalf("Close() error = %v", closeErr)
	}

	_, err = Open(t.Context(), options)
	var schemaErr *SchemaError
	if !errors.As(err, &schemaErr) {
		t.Fatalf("Open() error = %v, want *SchemaError", err)
	}
}

func TestOpenConcurrentMigration(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state")
	const workers = 4
	start := make(chan struct{})
	stores := make(chan *Store, workers)
	errorsFound := make(chan error, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			<-start
			store, err := Open(t.Context(), Options{
				StateDir:    stateDir,
				BusyTimeout: 30 * time.Second,
			})
			if err != nil {
				errorsFound <- err

				return
			}
			stores <- store
		}()
	}
	close(start)
	group.Wait()
	close(stores)
	close(errorsFound)

	for err := range errorsFound {
		t.Errorf("concurrent Open() error = %v", err)
	}
	for store := range stores {
		if err := store.Close(); err != nil {
			t.Errorf("concurrent Close() error = %v", err)
		}
	}
}

func TestSQLiteDSNUsesAbsoluteFileURI(t *testing.T) {
	t.Parallel()

	databasePath := filepath.Join(t.TempDir(), DatabaseFilename)
	dsn := sqliteDSN(databasePath, defaultBusyTimeout)
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse sqlite DSN %q: %v", dsn, err)
	}
	wantPath := sqliteURIPath(databasePath)
	if parsed.Scheme != "file" || parsed.Path != wantPath || parsed.Query().Get("_txlock") != "immediate" {
		t.Fatalf("sqliteDSN() = %q, parsed as %#v; want absolute path %q", dsn, parsed, wantPath)
	}
}

func TestParseSQLiteVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    [3]int
		wantErr bool
	}{
		{name: "minimum", input: "3.51.3", want: [3]int{3, 51, 3}},
		{name: "newer", input: "3.53.1", want: [3]int{3, 53, 1}},
		{name: "suffix component", input: "3.53.1.0", want: [3]int{3, 53, 1}},
		{name: "short", input: "3.51", wantErr: true},
		{name: "nonnumeric", input: "3.x.1", wantErr: true},
		{name: "negative", input: "3.-1.1", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			major, minor, patch, err := parseSQLiteVersion(test.input)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseSQLiteVersion(%q) error = %v, wantErr %v", test.input, err, test.wantErr)
			}
			if got := [3]int{major, minor, patch}; got != test.want {
				t.Errorf("parseSQLiteVersion(%q) = %v, want %v", test.input, got, test.want)
			}
		})
	}
}

func assertPragmaInt(t *testing.T, db *sql.DB, name string, want int) {
	t.Helper()

	var got int
	if err := db.QueryRowContext(t.Context(), "PRAGMA "+name).Scan(&got); err != nil {
		t.Fatalf("query PRAGMA %s: %v", name, err)
	}
	if got != want {
		t.Errorf("PRAGMA %s = %d, want %d", name, got, want)
	}
}

func assertPragmaText(t *testing.T, db *sql.DB, name, want string) {
	t.Helper()

	var got string
	if err := db.QueryRowContext(t.Context(), "PRAGMA "+name).Scan(&got); err != nil {
		t.Fatalf("query PRAGMA %s: %v", name, err)
	}
	if !strings.EqualFold(got, want) {
		t.Errorf("PRAGMA %s = %q, want %q", name, got, want)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Errorf("mode of %q = %04o, want %04o", path, got, want)
	}
}

func prepareRawDatabase(t *testing.T, stateDir string) string {
	t.Helper()

	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("create raw state directory: %v", err)
	}
	if err := hardenPath(stateDir); err != nil {
		t.Fatalf("harden raw state directory: %v", err)
	}
	path := filepath.Join(stateDir, DatabaseFilename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("create raw database: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close raw database file: %v", err)
	}
	if err := hardenPath(path); err != nil {
		t.Fatalf("harden raw database file: %v", err)
	}

	return path
}

func openRawDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}

	return db
}
