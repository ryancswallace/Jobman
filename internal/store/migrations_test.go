package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryancswallace/jobman/internal/model"
)

func TestMigrationSevenRepairsRuntimeCountersFromVersionOne(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatalf("create version-one state directory: %v", err)
	}
	if err := hardenPath(stateDir); err != nil {
		t.Fatalf("harden version-one state directory: %v", err)
	}
	databasePath := filepath.Join(stateDir, DatabaseFilename)
	database, err := sql.Open("sqlite", sqliteDSN(databasePath, defaultBusyTimeout))
	if err != nil {
		t.Fatalf("open version-one database: %v", err)
	}
	if _, err = database.ExecContext(t.Context(), migration1SQL); err != nil {
		t.Fatalf("apply migration one: %v", err)
	}
	if _, err = database.ExecContext(t.Context(), `
		INSERT INTO schema_migrations(version, applied_at_ns, jobman_version, checksum)
		VALUES (1, 1, 'test-v1', ?)`, migrationChecksum(migration1SQL)); err != nil {
		t.Fatalf("record migration one: %v", err)
	}
	if _, err = database.ExecContext(t.Context(), fmt.Sprintf("PRAGMA application_id = %d", applicationID)); err != nil {
		t.Fatalf("set application ID: %v", err)
	}
	if _, err = database.ExecContext(t.Context(), "PRAGMA user_version = 1"); err != nil {
		t.Fatalf("set version-one header: %v", err)
	}
	jobID := "018f0000-0000-7000-8000-000000000001"
	if _, err = database.ExecContext(t.Context(), `
		INSERT INTO jobs(id, spec_json, phase, outcome, revision, submitted_at_ns, completed_at_ns)
		VALUES (?, '{}', 'completed', 'failure', 1, 10, 50)`, jobID); err != nil {
		t.Fatalf("insert version-one job: %v", err)
	}
	for index, outcome := range []string{"success", "failure", "timed_out"} {
		runID := fmt.Sprintf("018f0000-0000-7000-8000-%012d", index+2)
		if _, err = database.ExecContext(t.Context(), `
			INSERT INTO runs(
				id, job_id, run_number, phase, outcome, revision, reserved_at_ns,
				completed_at_ns, stdout_path, stderr_path, index_path,
				log_index_version, log_integrity, recording_health
			) VALUES (?, ?, ?, 'completed', ?, 1, ?, ?, 'stdout', 'stderr', 'index', 1, 'valid', 'healthy')`,
			runID, jobID, index+1, outcome, 20+index, 30+index); err != nil {
			t.Fatalf("insert version-one run %d: %v", index+1, err)
		}
	}
	if err = database.Close(); err != nil {
		t.Fatalf("close version-one database: %v", err)
	}
	if err = os.Chmod(databasePath, 0o600); err != nil {
		t.Fatalf("make version-one database private: %v", err)
	}
	if err = hardenPath(databasePath); err != nil {
		t.Fatalf("harden version-one database: %v", err)
	}

	upgraded, err := Open(t.Context(), Options{
		StateDir: stateDir, JobmanVersion: "test", EventIDs: newSequentialEventIDs(0x9180),
	})
	if err != nil {
		t.Fatalf("Open(version one) error = %v", err)
	}
	if upgraded.LastBackupPath() == "" {
		t.Fatal("Open(version one) did not create a migration backup")
	}
	if _, statErr := os.Stat(upgraded.LastBackupPath()); statErr != nil {
		t.Fatalf("inspect migration backup: %v", statErr)
	}
	backup, err := sql.Open("sqlite", sqliteDSN(upgraded.LastBackupPath(), defaultBusyTimeout))
	if err != nil {
		t.Fatalf("open migration backup: %v", err)
	}
	assertPragmaInt(t, backup, "user_version", 1)
	if closeErr := backup.Close(); closeErr != nil {
		t.Fatalf("close migration backup: %v", closeErr)
	}
	defer func() {
		if closeErr := upgraded.Close(); closeErr != nil {
			t.Errorf("close upgraded store: %v", closeErr)
		}
	}()
	parsedJobID, err := model.ParseJobID(jobID)
	if err != nil {
		t.Fatalf("parse fixture job ID: %v", err)
	}
	runtimeState, err := upgraded.GetRuntime(t.Context(), parsedJobID)
	if err != nil {
		t.Fatalf("GetRuntime() error = %v", err)
	}
	if runtimeState.RunCount != 3 || runtimeState.SuccessCount != 1 || runtimeState.FailureCount != 2 {
		t.Fatalf("backfilled runtime = %#v", runtimeState)
	}
}

func TestMigrationCatalogIsContiguousAndChecksummed(t *testing.T) {
	t.Parallel()

	if len(migrations) != currentSchemaVersion {
		t.Fatalf("migration count = %d, current schema version = %d", len(migrations), currentSchemaVersion)
	}
	seenChecksums := make(map[string]int, len(migrations))
	for index, item := range migrations {
		wantVersion := index + 1
		if item.version != wantVersion {
			t.Errorf("migration[%d].version = %d, want %d", index, item.version, wantVersion)
		}
		if strings.TrimSpace(item.sql) == "" {
			t.Errorf("migration %d has no SQL", item.version)
		}

		checksum := migrationChecksum(item.sql)
		digest, err := hex.DecodeString(checksum)
		if err != nil || len(digest) != sha256.Size {
			t.Errorf("migration %d checksum = %q, want SHA-256 hex", item.version, checksum)
		}
		if prior, exists := seenChecksums[checksum]; exists {
			t.Errorf("migrations %d and %d have the same checksum", prior, item.version)
		}
		seenChecksums[checksum] = item.version
	}
}

func TestOpenRejectsIncompleteMigrationHistory(t *testing.T) {
	t.Parallel()

	options := Options{StateDir: filepath.Join(t.TempDir(), "state")}
	store, err := Open(t.Context(), options)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, deleteErr := store.db.ExecContext(t.Context(), "DELETE FROM schema_migrations WHERE version = 1"); deleteErr != nil {
		t.Fatalf("delete migration history: %v", deleteErr)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatalf("Close() error = %v", closeErr)
	}

	reopened, err := Open(t.Context(), options)
	if reopened != nil {
		if closeErr := reopened.Close(); closeErr != nil {
			t.Errorf("close unexpectedly opened store: %v", closeErr)
		}
	}
	var schemaErr *SchemaError
	if !errors.As(err, &schemaErr) {
		t.Fatalf("Open() error = %v, want *SchemaError", err)
	}
}

func TestMigratedSchemaObjectsAndIntegrity(t *testing.T) {
	t.Parallel()

	store := openTestStore(t, "schema-invariants", newSequentialEventIDs(0x9000))
	assertStrictSchemaTables(t, store)
	assertSchemaIndexes(t, store)
	assertDatabaseIntegrity(t, store)
}

func TestMigrationSixUpgradesNotificationQueueFromVersionFive(t *testing.T) {
	t.Parallel()

	options := Options{
		StateDir: filepath.Join(t.TempDir(), "state"), EventIDs: newSequentialEventIDs(0x9100),
	}
	database, openErr := Open(t.Context(), options)
	if openErr != nil {
		t.Fatalf("Open() error = %v", openErr)
	}
	if _, execErr := database.db.ExecContext(t.Context(), "DROP TABLE notification_deliveries"); execErr != nil {
		t.Fatalf("remove migration-six table: %v", execErr)
	}
	if _, execErr := database.db.ExecContext(t.Context(), "DROP TABLE run_diagnostic_facts"); execErr != nil {
		t.Fatalf("remove migration-eight facts table: %v", execErr)
	}
	if _, execErr := database.db.ExecContext(t.Context(), "DROP TABLE store_secrets"); execErr != nil {
		t.Fatalf("remove migration-eight secret table: %v", execErr)
	}
	if _, execErr := database.db.ExecContext(t.Context(), "DELETE FROM schema_migrations WHERE version >= 6"); execErr != nil {
		t.Fatalf("remove post-version-five migration history: %v", execErr)
	}
	if _, execErr := database.db.ExecContext(t.Context(), fmt.Sprintf("PRAGMA user_version = %d", 5)); execErr != nil {
		t.Fatalf("restore version-five header: %v", execErr)
	}
	if closeErr := database.Close(); closeErr != nil {
		t.Fatalf("close version-five store: %v", closeErr)
	}

	upgraded, openErr := Open(t.Context(), options)
	if openErr != nil {
		t.Fatalf("Open(version five) error = %v", openErr)
	}
	defer func() {
		if err := upgraded.Close(); err != nil {
			t.Errorf("close upgraded store: %v", err)
		}
	}()
	assertPragmaInt(t, upgraded.db, "user_version", currentSchemaVersion)
	var tableCount int
	if err := upgraded.db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'table' AND name = 'notification_deliveries'`).Scan(&tableCount); err != nil {
		t.Fatalf("inspect upgraded notification queue: %v", err)
	}
	if tableCount != 1 {
		t.Fatalf("notification_deliveries table count = %d, want 1", tableCount)
	}
}

func assertStrictSchemaTables(t *testing.T, store *Store) {
	t.Helper()

	wantTables := map[string]bool{
		"admission_requests":      false,
		"admissions":              false,
		"concurrency_limits":      false,
		"job_dependencies":        false,
		"job_runtime":             false,
		"job_tags":                false,
		"jobs":                    false,
		"notification_attempts":   false,
		"notification_deliveries": false,
		"run_diagnostic_facts":    false,
		"run_log_pruning":         false,
		"runs":                    false,
		"schema_migrations":       false,
		"state_events":            false,
		"store_secrets":           false,
		"supervisors":             false,
		"wait_evaluations":        false,
	}
	rows, queryErr := store.db.QueryContext(t.Context(), `
		SELECT name, sql
		FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	if queryErr != nil {
		t.Fatalf("query schema tables: %v", queryErr)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var definition string
		if scanErr := rows.Scan(&name, &definition); scanErr != nil {
			t.Fatalf("scan schema table: %v", scanErr)
		}
		if _, expected := wantTables[name]; !expected {
			t.Fatalf("unexpected schema table %q", name)
		}
		wantTables[name] = true
		if !strings.HasSuffix(strings.TrimSpace(definition), "STRICT") {
			t.Errorf("schema table %q is not STRICT: %s", name, definition)
		}
	}
	if iterationErr := rows.Err(); iterationErr != nil {
		t.Fatalf("iterate schema tables: %v", iterationErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		t.Fatalf("close schema table rows: %v", closeErr)
	}
	for name, found := range wantTables {
		if !found {
			t.Errorf("schema table %q is missing", name)
		}
	}
}

func assertSchemaIndexes(t *testing.T, store *Store) {
	t.Helper()

	wantIndexes := map[string]bool{
		"admission_requests_order":         false,
		"admissions_active_global":         false,
		"admissions_active_pool":           false,
		"events_job":                       false,
		"job_dependencies_target":          false,
		"job_tags_tag":                     false,
		"jobs_name":                        false,
		"jobs_phase":                       false,
		"jobs_submitted_order":             false,
		"runs_job":                         false,
		"runs_one_active_per_job":          false,
		"notification_attempts_pending":    false,
		"notification_deliveries_job":      false,
		"notification_deliveries_ready":    false,
		"run_diagnostic_facts_fingerprint": false,
	}
	rows, queryErr := store.db.QueryContext(t.Context(), `
		SELECT name
		FROM sqlite_schema
		WHERE type = 'index' AND name NOT LIKE 'sqlite_%'`)
	if queryErr != nil {
		t.Fatalf("query schema indexes: %v", queryErr)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if scanErr := rows.Scan(&name); scanErr != nil {
			t.Fatalf("scan schema index: %v", scanErr)
		}
		if _, expected := wantIndexes[name]; !expected {
			t.Fatalf("unexpected schema index %q", name)
		}
		wantIndexes[name] = true
	}
	if iterationErr := rows.Err(); iterationErr != nil {
		t.Fatalf("iterate schema indexes: %v", iterationErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		t.Fatalf("close schema index rows: %v", closeErr)
	}
	for name, found := range wantIndexes {
		if !found {
			t.Errorf("schema index %q is missing", name)
		}
	}
}

func assertDatabaseIntegrity(t *testing.T, store *Store) {
	t.Helper()

	var integrity string
	if queryErr := store.db.QueryRowContext(t.Context(), "PRAGMA integrity_check").Scan(&integrity); queryErr != nil {
		t.Fatalf("run SQLite integrity check: %v", queryErr)
	}
	if integrity != "ok" {
		t.Errorf("SQLite integrity check = %q, want ok", integrity)
	}

	rows, queryErr := store.db.QueryContext(t.Context(), "PRAGMA foreign_key_check")
	if queryErr != nil {
		t.Fatalf("run SQLite foreign-key check: %v", queryErr)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("SQLite foreign-key check reported a violation")
	}
	if iterationErr := rows.Err(); iterationErr != nil {
		t.Fatalf("iterate SQLite foreign-key check: %v", iterationErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		t.Fatalf("close SQLite foreign-key rows: %v", closeErr)
	}
}
