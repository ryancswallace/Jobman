package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
)

const (
	applicationID        = 0x4a4f424d // "JOBM" in big-endian ASCII.
	currentSchemaVersion = 8
)

const migration1SQL = `
CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    applied_at_ns INTEGER NOT NULL CHECK (applied_at_ns >= 0),
    jobman_version TEXT NOT NULL,
    checksum TEXT NOT NULL CHECK (length(checksum) = 64)
) STRICT;

CREATE TABLE jobs (
    id TEXT PRIMARY KEY CHECK (length(id) = 36),
    name TEXT,
    spec_json TEXT NOT NULL CHECK (json_valid(spec_json)),
    phase TEXT NOT NULL CHECK (phase IN (
        'submitting', 'waiting', 'queued', 'starting', 'running',
        'backoff', 'paused', 'stopping', 'completed'
    )),
    outcome TEXT CHECK (outcome IS NULL OR outcome IN (
        'success', 'failure', 'timed_out', 'cancelled', 'aborted',
        'lost', 'submission_failed'
    )),
    revision INTEGER NOT NULL CHECK (revision > 0),
    submitted_at_ns INTEGER NOT NULL CHECK (submitted_at_ns >= 0),
    claimed_at_ns INTEGER CHECK (claimed_at_ns IS NULL OR claimed_at_ns >= submitted_at_ns),
    started_at_ns INTEGER CHECK (started_at_ns IS NULL OR started_at_ns >= submitted_at_ns),
    completed_at_ns INTEGER CHECK (completed_at_ns IS NULL OR completed_at_ns >= submitted_at_ns),
    active_run_id TEXT REFERENCES runs(id) DEFERRABLE INITIALLY DEFERRED,
    supervisor_id TEXT REFERENCES supervisors(id) DEFERRABLE INITIALLY DEFERRED,
    cancellation_requested_at_ns INTEGER CHECK (
        cancellation_requested_at_ns IS NULL OR cancellation_requested_at_ns >= submitted_at_ns
    ),
    cancellation_reason TEXT,
    last_diagnostic_code TEXT,
    launch_credential_hash BLOB CHECK (
        launch_credential_hash IS NULL OR length(launch_credential_hash) = 32
    ),
    claim_deadline_ns INTEGER CHECK (claim_deadline_ns IS NULL OR claim_deadline_ns >= submitted_at_ns),
    CHECK ((phase = 'completed') = (outcome IS NOT NULL)),
    CHECK ((outcome IS NULL) = (completed_at_ns IS NULL)),
    CHECK ((launch_credential_hash IS NULL) = (claim_deadline_ns IS NULL)),
    CHECK (cancellation_reason IS NULL OR cancellation_requested_at_ns IS NOT NULL)
) STRICT;

CREATE TABLE runs (
    id TEXT PRIMARY KEY CHECK (length(id) = 36),
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    run_number INTEGER NOT NULL CHECK (run_number > 0),
    phase TEXT NOT NULL CHECK (phase IN ('starting', 'running', 'paused', 'stopping', 'completed')),
    outcome TEXT CHECK (outcome IS NULL OR outcome IN (
        'success', 'failure', 'timed_out', 'cancelled', 'start_failed', 'lost'
    )),
    revision INTEGER NOT NULL CHECK (revision > 0),
    resolved_executable TEXT,
    reserved_at_ns INTEGER NOT NULL CHECK (reserved_at_ns >= 0),
    started_at_ns INTEGER CHECK (started_at_ns IS NULL OR started_at_ns >= reserved_at_ns),
    stop_requested_at_ns INTEGER CHECK (stop_requested_at_ns IS NULL OR stop_requested_at_ns >= reserved_at_ns),
    stop_reason TEXT CHECK (stop_reason IS NULL OR stop_reason IN ('cancellation', 'timeout')),
    completed_at_ns INTEGER CHECK (completed_at_ns IS NULL OR completed_at_ns >= reserved_at_ns),
    process_pid INTEGER CHECK (process_pid IS NULL OR process_pid > 0),
    process_identity_json TEXT CHECK (
        process_identity_json IS NULL OR json_valid(process_identity_json)
    ),
    exit_code INTEGER CHECK (exit_code IS NULL OR exit_code >= 0),
    exit_signal TEXT,
    exit_platform_reason TEXT,
    exit_observed_at_ns INTEGER CHECK (exit_observed_at_ns IS NULL OR exit_observed_at_ns >= reserved_at_ns),
    stdout_path TEXT NOT NULL,
    stderr_path TEXT NOT NULL,
    index_path TEXT NOT NULL,
    stdout_size INTEGER NOT NULL DEFAULT 0 CHECK (stdout_size >= 0),
    stderr_size INTEGER NOT NULL DEFAULT 0 CHECK (stderr_size >= 0),
    log_index_version INTEGER NOT NULL CHECK (log_index_version > 0),
    log_integrity TEXT NOT NULL CHECK (log_integrity IN ('pending', 'valid', 'partial', 'corrupt')),
    recording_health TEXT NOT NULL CHECK (recording_health IN ('healthy', 'degraded')),
    log_diagnostic_code TEXT,
    last_diagnostic_code TEXT,
    UNIQUE (job_id, run_number),
    CHECK ((phase = 'completed') = (outcome IS NOT NULL)),
    CHECK ((outcome IS NULL) = (completed_at_ns IS NULL)),
    CHECK ((process_pid IS NULL) = (process_identity_json IS NULL)),
    CHECK (exit_code IS NULL OR outcome IN ('success', 'failure')),
    CHECK ((stop_reason IS NULL) = (stop_requested_at_ns IS NULL)),
    CHECK ((exit_observed_at_ns IS NULL) = (
        exit_code IS NULL AND exit_signal IS NULL AND exit_platform_reason IS NULL
    ))
) STRICT;

CREATE UNIQUE INDEX runs_one_active_per_job
    ON runs(job_id) WHERE phase != 'completed';

CREATE TABLE supervisors (
    id TEXT PRIMARY KEY CHECK (length(id) = 36),
    job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id) ON DELETE RESTRICT,
    revision INTEGER NOT NULL CHECK (revision > 0),
    process_pid INTEGER NOT NULL CHECK (process_pid > 0),
    process_identity_json TEXT NOT NULL CHECK (json_valid(process_identity_json)),
    claimed_at_ns INTEGER NOT NULL CHECK (claimed_at_ns >= 0),
    lease_renewed_at_ns INTEGER NOT NULL CHECK (lease_renewed_at_ns >= claimed_at_ns),
    lease_expires_at_ns INTEGER NOT NULL CHECK (lease_expires_at_ns >= lease_renewed_at_ns),
    released_at_ns INTEGER CHECK (released_at_ns IS NULL OR released_at_ns >= claimed_at_ns)
) STRICT;

CREATE TABLE state_events (
    id TEXT PRIMARY KEY CHECK (length(id) = 36),
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
    supervisor_id TEXT REFERENCES supervisors(id) ON DELETE RESTRICT,
    entity_kind TEXT NOT NULL CHECK (entity_kind IN ('job', 'run', 'supervisor')),
    entity_id TEXT NOT NULL CHECK (length(entity_id) = 36),
    event_type TEXT NOT NULL,
    from_phase TEXT,
    to_phase TEXT NOT NULL,
    from_outcome TEXT,
    to_outcome TEXT,
    entity_revision INTEGER NOT NULL CHECK (entity_revision > 0),
    occurred_at_ns INTEGER NOT NULL CHECK (occurred_at_ns >= 0),
    details_json TEXT NOT NULL CHECK (json_valid(details_json)),
    UNIQUE (entity_kind, entity_id, entity_revision)
) STRICT;

CREATE INDEX jobs_submitted_order ON jobs(submitted_at_ns DESC, id DESC);
CREATE INDEX jobs_name ON jobs(name) WHERE name IS NOT NULL;
CREATE INDEX jobs_phase ON jobs(phase, submitted_at_ns DESC);
CREATE INDEX runs_job ON runs(job_id, run_number);
CREATE INDEX events_job ON state_events(job_id, occurred_at_ns, id);
`

const migration2SQL = `
CREATE TABLE job_runtime (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    run_count INTEGER NOT NULL DEFAULT 0 CHECK (run_count >= 0),
    success_count INTEGER NOT NULL DEFAULT 0 CHECK (success_count >= 0),
    failure_count INTEGER NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
    next_run_at_ns INTEGER CHECK (next_run_at_ns IS NULL OR next_run_at_ns >= 0),
    waiting_reason TEXT,
    paused_from_phase TEXT CHECK (paused_from_phase IS NULL OR paused_from_phase IN (
        'waiting', 'queued', 'starting', 'running', 'backoff'
    )),
    paused_at_ns INTEGER CHECK (paused_at_ns IS NULL OR paused_at_ns >= 0),
    total_paused_ns INTEGER NOT NULL DEFAULT 0 CHECK (total_paused_ns >= 0),
    prerequisites_satisfied_at_ns INTEGER CHECK (
        prerequisites_satisfied_at_ns IS NULL OR prerequisites_satisfied_at_ns >= 0
    ),
    input_endpoint TEXT,
    input_eof_requested INTEGER NOT NULL DEFAULT 0 CHECK (input_eof_requested IN (0, 1)),
    updated_at_ns INTEGER NOT NULL CHECK (updated_at_ns >= 0),
    CHECK ((paused_from_phase IS NULL) = (paused_at_ns IS NULL)),
    CHECK (success_count + failure_count <= run_count)
) STRICT;

INSERT INTO job_runtime(job_id, updated_at_ns)
SELECT id, submitted_at_ns FROM jobs;

CREATE TABLE job_dependencies (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    dependency_job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    predicate TEXT NOT NULL CHECK (predicate IN (
        'success', 'finish', 'failed', 'timed_out', 'cancelled',
        'aborted', 'lost', 'submission_failed'
    )),
    observed_revision INTEGER CHECK (observed_revision IS NULL OR observed_revision > 0),
    observed_outcome TEXT,
    satisfied_at_ns INTEGER CHECK (satisfied_at_ns IS NULL OR satisfied_at_ns >= 0),
    PRIMARY KEY (job_id, dependency_job_id, predicate),
    CHECK (job_id != dependency_job_id),
    CHECK ((observed_revision IS NULL) = (observed_outcome IS NULL))
) STRICT;

CREATE INDEX job_dependencies_target
    ON job_dependencies(dependency_job_id, job_id);

CREATE TABLE wait_evaluations (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    condition_index INTEGER NOT NULL CHECK (condition_index >= 0),
    condition_kind TEXT NOT NULL CHECK (condition_kind IN ('until', 'delay', 'file_exists', 'probe')),
    evaluated_at_ns INTEGER CHECK (evaluated_at_ns IS NULL OR evaluated_at_ns >= 0),
    satisfied_at_ns INTEGER CHECK (satisfied_at_ns IS NULL OR satisfied_at_ns >= 0),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_diagnostic_code TEXT,
    PRIMARY KEY (job_id, condition_index)
) STRICT;

CREATE TABLE concurrency_limits (
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('global', 'pool')),
    scope_name TEXT NOT NULL,
    capacity INTEGER CHECK (capacity IS NULL OR capacity > 0),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at_ns INTEGER NOT NULL CHECK (updated_at_ns >= 0),
    PRIMARY KEY (scope_kind, scope_name),
    CHECK ((scope_kind = 'global') = (scope_name = ''))
) STRICT;

CREATE TABLE admissions (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES runs(id) ON DELETE SET NULL,
    pool_name TEXT,
    slots INTEGER NOT NULL CHECK (slots > 0),
    acquired_at_ns INTEGER NOT NULL CHECK (acquired_at_ns >= 0),
    lease_expires_at_ns INTEGER NOT NULL CHECK (lease_expires_at_ns > acquired_at_ns),
    released_at_ns INTEGER CHECK (released_at_ns IS NULL OR released_at_ns >= acquired_at_ns)
) STRICT;

CREATE INDEX admissions_active_global
    ON admissions(released_at_ns, lease_expires_at_ns);
CREATE INDEX admissions_active_pool
    ON admissions(pool_name, released_at_ns, lease_expires_at_ns);

CREATE TABLE notification_attempts (
    id TEXT PRIMARY KEY CHECK (length(id) = 36),
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL CHECK (length(event_id) = 36),
    notifier_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'delivering', 'succeeded', 'failed')),
    created_at_ns INTEGER NOT NULL CHECK (created_at_ns >= 0),
    started_at_ns INTEGER CHECK (started_at_ns IS NULL OR started_at_ns >= created_at_ns),
    completed_at_ns INTEGER CHECK (completed_at_ns IS NULL OR completed_at_ns >= created_at_ns),
    next_attempt_at_ns INTEGER CHECK (next_attempt_at_ns IS NULL OR next_attempt_at_ns >= created_at_ns),
    diagnostic_code TEXT,
    retryable INTEGER NOT NULL DEFAULT 0 CHECK (retryable IN (0, 1)),
    response_status_code INTEGER CHECK (
        response_status_code IS NULL OR response_status_code BETWEEN 100 AND 999
    ),
    command_exit_code INTEGER,
    message_id TEXT,
    response_truncated INTEGER NOT NULL DEFAULT 0 CHECK (response_truncated IN (0, 1)),
    UNIQUE (event_id, notifier_name, attempt_number),
    CHECK (status != 'succeeded' OR diagnostic_code IS NULL),
    CHECK (status != 'succeeded' OR retryable = 0)
) STRICT;

CREATE INDEX notification_attempts_pending
    ON notification_attempts(status, next_attempt_at_ns, created_at_ns);

CREATE TABLE job_tags (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    tag TEXT NOT NULL CHECK (tag != ''),
    PRIMARY KEY (job_id, tag)
) STRICT;

CREATE INDEX job_tags_tag ON job_tags(tag, job_id);
`

const migration3SQL = `
ALTER TABLE job_dependencies RENAME TO job_dependencies_v2;
DROP INDEX job_dependencies_target;

CREATE TABLE job_dependencies (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    dependency_job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    predicate TEXT NOT NULL CHECK (
        predicate IN (
            'success', 'finish', 'failed', 'timed_out', 'cancelled',
            'aborted', 'lost', 'submission_failed'
        ) OR (
            predicate GLOB 'outcomes:*' AND length(predicate) BETWEEN 10 AND 256
        )
    ),
    observed_revision INTEGER CHECK (observed_revision IS NULL OR observed_revision > 0),
    observed_outcome TEXT,
    satisfied_at_ns INTEGER CHECK (satisfied_at_ns IS NULL OR satisfied_at_ns >= 0),
    PRIMARY KEY (job_id, dependency_job_id, predicate),
    CHECK (job_id != dependency_job_id),
    CHECK ((observed_revision IS NULL) = (observed_outcome IS NULL))
) STRICT;

INSERT INTO job_dependencies(
    job_id, dependency_job_id, predicate, observed_revision, observed_outcome, satisfied_at_ns
)
SELECT job_id, dependency_job_id, predicate, observed_revision, observed_outcome, satisfied_at_ns
FROM job_dependencies_v2;

DROP TABLE job_dependencies_v2;

CREATE INDEX job_dependencies_target
    ON job_dependencies(dependency_job_id, job_id);
`

const migration4SQL = `
CREATE TABLE admission_requests (
    sequence INTEGER PRIMARY KEY,
    job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id) ON DELETE CASCADE,
    pool_name TEXT,
    slots INTEGER NOT NULL CHECK (slots > 0),
    enqueued_at_ns INTEGER NOT NULL CHECK (enqueued_at_ns >= 0),
    bypass_count INTEGER NOT NULL DEFAULT 0 CHECK (bypass_count >= 0),
    CHECK (pool_name IS NULL OR pool_name != '')
) STRICT;

CREATE INDEX admission_requests_order
    ON admission_requests(sequence);

CREATE TRIGGER admission_requests_terminal_cleanup
AFTER UPDATE OF phase ON jobs
WHEN NEW.phase = 'completed' AND OLD.phase != 'completed'
BEGIN
    DELETE FROM admission_requests WHERE job_id = NEW.id;
END;
`

const migration5SQL = `
CREATE TABLE run_log_pruning (
    run_id TEXT PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    pruned_at_ns INTEGER NOT NULL CHECK (pruned_at_ns >= 0),
    removed_files INTEGER NOT NULL CHECK (removed_files >= 0),
    removed_bytes INTEGER NOT NULL CHECK (removed_bytes >= 0)
) STRICT;
`

const migration6SQL = `
CREATE TABLE notification_deliveries (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES state_events(id) ON DELETE CASCADE,
    notifier_name TEXT NOT NULL CHECK (
        length(notifier_name) BETWEEN 1 AND 128 AND trim(notifier_name) = notifier_name
    ),
    event_type TEXT NOT NULL,
    run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
    occurred_at_ns INTEGER NOT NULL CHECK (occurred_at_ns >= 0),
    created_at_ns INTEGER NOT NULL CHECK (created_at_ns >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (
        attempt_count >= 0 AND attempt_count <= max_attempts
    ),
    status TEXT NOT NULL CHECK (status IN ('pending', 'delivering', 'succeeded', 'failed')),
    next_attempt_at_ns INTEGER CHECK (next_attempt_at_ns IS NULL OR next_attempt_at_ns >= created_at_ns),
    claim_token TEXT CHECK (claim_token IS NULL OR length(claim_token) = 36),
    claimed_at_ns INTEGER CHECK (claimed_at_ns IS NULL OR claimed_at_ns >= created_at_ns),
    claim_expires_at_ns INTEGER CHECK (
        claim_expires_at_ns IS NULL OR claim_expires_at_ns > claimed_at_ns
    ),
    completed_at_ns INTEGER CHECK (completed_at_ns IS NULL OR completed_at_ns >= created_at_ns),
    PRIMARY KEY (event_id, notifier_name),
    CHECK (
        (status = 'pending' AND next_attempt_at_ns IS NOT NULL AND
            claim_token IS NULL AND claimed_at_ns IS NULL AND
            claim_expires_at_ns IS NULL AND completed_at_ns IS NULL) OR
        (status = 'delivering' AND next_attempt_at_ns IS NULL AND
            claim_token IS NOT NULL AND claimed_at_ns IS NOT NULL AND
            claim_expires_at_ns IS NOT NULL AND completed_at_ns IS NULL) OR
        (status IN ('succeeded', 'failed') AND next_attempt_at_ns IS NULL AND
            claim_token IS NULL AND claimed_at_ns IS NULL AND
            claim_expires_at_ns IS NULL AND completed_at_ns IS NOT NULL AND
            attempt_count > 0)
    )
) STRICT;

CREATE INDEX notification_deliveries_ready
    ON notification_deliveries(status, next_attempt_at_ns, claim_expires_at_ns, created_at_ns);
CREATE INDEX notification_deliveries_job
    ON notification_deliveries(job_id, status, created_at_ns);
`

const migration7SQL = `
UPDATE job_runtime
SET run_count = (
        SELECT COUNT(*) FROM runs
        WHERE runs.job_id = job_runtime.job_id AND runs.phase = 'completed'
    ),
    success_count = (
        SELECT COUNT(*) FROM runs
        WHERE runs.job_id = job_runtime.job_id
          AND runs.phase = 'completed' AND runs.outcome = 'success'
    ),
    failure_count = (
        SELECT COUNT(*) FROM runs
        WHERE runs.job_id = job_runtime.job_id
          AND runs.phase = 'completed' AND runs.outcome != 'success'
    ),
    updated_at_ns = MAX(
        updated_at_ns,
        COALESCE((
            SELECT MAX(runs.completed_at_ns) FROM runs
            WHERE runs.job_id = job_runtime.job_id AND runs.phase = 'completed'
        ), updated_at_ns)
    );

DROP INDEX admission_requests_order;
CREATE INDEX admission_requests_order
    ON admission_requests(enqueued_at_ns, job_id);
`

const migration8SQL = `
CREATE TABLE store_secrets (
    name TEXT PRIMARY KEY CHECK (name = 'failure_fingerprint_hmac_v1'),
    value BLOB NOT NULL CHECK (length(value) = 32),
    created_at_ns INTEGER NOT NULL CHECK (created_at_ns >= 0)
) STRICT;

CREATE TABLE run_diagnostic_facts (
    run_id TEXT PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    resources_json TEXT NOT NULL CHECK (
        json_valid(resources_json) AND json_type(resources_json) = 'array' AND
        length(CAST(resources_json AS BLOB)) <= 16384
    ),
    failure_class TEXT CHECK (
        failure_class IS NULL OR (
            length(failure_class) BETWEEN 1 AND 128 AND
            failure_class NOT GLOB '*[^a-z0-9_]*'
        )
    ),
    fingerprint_algorithm TEXT CHECK (
        fingerprint_algorithm IS NULL OR fingerprint_algorithm = 'hmac-sha256'
    ),
    fingerprint_input_version INTEGER CHECK (
        fingerprint_input_version IS NULL OR fingerprint_input_version = 1
    ),
    fingerprint_value BLOB CHECK (
        fingerprint_value IS NULL OR length(fingerprint_value) = 32
    ),
    recorded_at_ns INTEGER NOT NULL CHECK (recorded_at_ns >= 0),
    CHECK (
        (
            fingerprint_algorithm IS NULL AND
            fingerprint_input_version IS NULL AND
            fingerprint_value IS NULL
        ) OR (
            fingerprint_algorithm IS NOT NULL AND
            fingerprint_input_version IS NOT NULL AND
            fingerprint_value IS NOT NULL
        )
    ),
    CHECK (fingerprint_value IS NULL OR failure_class IS NOT NULL)
) STRICT;

CREATE INDEX run_diagnostic_facts_fingerprint
    ON run_diagnostic_facts(
        fingerprint_algorithm,
        fingerprint_input_version,
        fingerprint_value,
        recorded_at_ns DESC,
        run_id DESC
    )
    WHERE fingerprint_value IS NOT NULL;
`

type migration struct {
	version int
	sql     string
}

var migrations = []migration{
	{version: 1, sql: migration1SQL},
	{version: 2, sql: migration2SQL},
	{version: 3, sql: migration3SQL},
	{version: 4, sql: migration4SQL},
	{version: 5, sql: migration5SQL},
	{version: 6, sql: migration6SQL},
	{version: 7, sql: migration7SQL},
	{version: 8, sql: migration8SQL},
}

func (s *Store) migrate(ctx context.Context) error {
	version, err := validateMigrationState(ctx, s.db)
	if err != nil || version == currentSchemaVersion {
		return err
	}

	return s.writeTransaction(ctx, "store migration", func(tx *sql.Tx) error {
		version, err := validateMigrationState(ctx, tx)
		if err != nil {
			return err
		}

		for _, item := range migrations {
			if item.version <= version {
				continue
			}
			if err := s.applyMigration(ctx, tx, item, version); err != nil {
				return err
			}
			version = item.version
		}

		return nil
	})
}

func validateMigrationState(ctx context.Context, queryer schemaQueryer) (int, error) {
	application, version, err := readSchemaHeaders(ctx, queryer)
	if err != nil {
		return 0, err
	}
	if err := validateSchemaHeaders(application, version); err != nil {
		return 0, err
	}
	if err := verifyAppliedMigrations(ctx, queryer, version); err != nil {
		return 0, err
	}

	return version, nil
}

func validateSchemaHeaders(application, version int) error {
	if application != 0 && application != applicationID {
		return &SchemaError{Reason: fmt.Sprintf("application id is %#x, want %#x", application, applicationID)}
	}
	if version < 0 || version > currentSchemaVersion {
		return &SchemaError{Reason: fmt.Sprintf("schema version is %d, newest supported is %d", version, currentSchemaVersion)}
	}
	if version > 0 && application != applicationID {
		return &SchemaError{Reason: fmt.Sprintf("schema version %d has no Jobman application id", version)}
	}

	return nil
}

func (s *Store) applyMigration(ctx context.Context, tx *sql.Tx, item migration, priorVersion int) error {
	if item.version != priorVersion+1 {
		return &SchemaError{Reason: fmt.Sprintf("migration sequence skips from %d to %d", priorVersion, item.version)}
	}
	if _, err := tx.ExecContext(ctx, item.sql); err != nil {
		return fmt.Errorf("apply database migration %d: %w", item.version, classifySQLite("apply database migration", err))
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO schema_migrations(version, applied_at_ns, jobman_version, checksum)
		 VALUES (?, ?, ?, ?)`,
		item.version,
		s.now().UTC().UnixNano(),
		s.jobmanVersion,
		migrationChecksum(item.sql),
	); err != nil {
		return fmt.Errorf("record database migration %d: %w", item.version, classifySQLite("record database migration", err))
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id = %d", applicationID)); err != nil {
		return fmt.Errorf("set database application id: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", item.version)); err != nil {
		return fmt.Errorf("set database schema version: %w", err)
	}

	return nil
}

type schemaQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readSchemaHeaders(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
},
) (application, version int, err error) {
	if err := queryer.QueryRowContext(ctx, "PRAGMA application_id").Scan(&application); err != nil {
		return 0, 0, fmt.Errorf("read database application id: %w", err)
	}
	if err := queryer.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return 0, 0, fmt.Errorf("read database schema version: %w", err)
	}

	return application, version, nil
}

func verifyAppliedMigrations(ctx context.Context, queryer schemaQueryer, version int) error {
	if version == 0 {
		return nil
	}

	rows, err := queryer.QueryContext(ctx, `
		SELECT version, checksum
		FROM schema_migrations
		ORDER BY version`)
	if err != nil {
		return fmt.Errorf("read migration history: %w", err)
	}
	defer rows.Close()

	seen := 0
	for rows.Next() {
		var migrationVersion int
		var checksum string
		if err := rows.Scan(&migrationVersion, &checksum); err != nil {
			return fmt.Errorf("decode migration history: %w", err)
		}
		seen++
		if migrationVersion != seen || migrationVersion > len(migrations) {
			return &SchemaError{Reason: fmt.Sprintf("migration history contains unexpected version %d", migrationVersion)}
		}
		want := migrationChecksum(migrations[migrationVersion-1].sql)
		if checksum != want {
			return &SchemaError{Reason: fmt.Sprintf("migration %d checksum does not match", migrationVersion)}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate migration history: %w", err)
	}
	if seen != version {
		return &SchemaError{Reason: fmt.Sprintf("migration history ends at %d but header reports %d", seen, version)}
	}

	return nil
}

func migrationChecksum(statement string) string {
	digest := sha256.Sum256([]byte(statement))

	return hex.EncodeToString(digest[:])
}

func (s *Store) verifySchema(ctx context.Context) error {
	application, version, err := readSchemaHeaders(ctx, s.db)
	if err != nil {
		return err
	}
	if application != applicationID {
		return &SchemaError{Reason: fmt.Sprintf("application id is %#x, want %#x", application, applicationID)}
	}
	if version != currentSchemaVersion {
		return &SchemaError{Reason: fmt.Sprintf("schema version is %d, want %d", version, currentSchemaVersion)}
	}

	if err := verifyAppliedMigrations(ctx, s.db, version); err != nil {
		return err
	}

	return s.verifyFingerprintKey(ctx)
}
