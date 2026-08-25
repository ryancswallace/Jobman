package agent

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/ryancswallace/jobman/internal/artifact"
	"github.com/ryancswallace/jobman/protocol"
)

const agentDatabaseFilename = "agent.db"

var errSpoolConflict = errors.New("agent spool record conflicts with durable state")

// Spool is the agent's host-local journal. It is authoritative for whether an
// assignment was durably recorded before acceptance and for unsent events.
type Spool struct {
	database *sql.DB
}

// SpoolExecution is one durable, non-terminal or recently terminal execution.
type SpoolExecution struct {
	Assignment    protocol.SealedAgentAssignment
	Authorization *protocol.LaunchAuthorization
	State         string
}

// PendingEvent is one execution event waiting for control-plane receipt.
type PendingEvent struct {
	Event protocol.SealedExecutionEvent
}

// OpenSpool creates or validates the private SQLite journal in stateDirectory.
func OpenSpool(ctx context.Context, stateDirectory string) (*Spool, error) {
	if ctx == nil {
		return nil, errors.New("open agent spool: nil context")
	}
	prepared, err := prepareStateDirectory(stateDirectory)
	if err != nil {
		return nil, fmt.Errorf("open agent spool: %w", err)
	}
	databasePath := filepath.Join(prepared, agentDatabaseFilename)
	file, err := os.OpenFile(databasePath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open agent spool file: %w", err)
	}
	if err = file.Close(); err != nil {
		return nil, fmt.Errorf("close agent spool file: %w", err)
	}

	database, err := sql.Open("sqlite", "file:"+databasePath+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=synchronous(FULL)")
	if err != nil {
		return nil, fmt.Errorf("open agent spool database: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	spool := &Spool{database: database}
	if err = spool.initialize(ctx); err != nil {
		return nil, errors.Join(err, database.Close())
	}

	return spool, nil
}

func (spool *Spool) initialize(ctx context.Context) error {
	var mode string
	if err := spool.database.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
		return fmt.Errorf("enable agent spool WAL: %w", err)
	}
	if mode != "wal" {
		return fmt.Errorf("enable agent spool WAL: database returned %q", mode)
	}
	const schema = `
CREATE TABLE IF NOT EXISTS executions (
    execution_id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL UNIQUE,
    assignment_document BLOB NOT NULL,
    assignment_digest TEXT NOT NULL,
    authorization_document BLOB,
    state TEXT NOT NULL CHECK (state IN ('offered', 'accepted', 'launching', 'running', 'terminal')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK ((state = 'offered') = (authorization_document IS NULL))
);
CREATE TABLE IF NOT EXISTS pending_events (
    event_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(execution_id),
    source_sequence INTEGER NOT NULL CHECK (source_sequence > 0),
    event_document BLOB NOT NULL,
    event_digest TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0 CHECK (delivered IN (0, 1)),
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, source_sequence)
);
CREATE INDEX IF NOT EXISTS pending_events_delivery_idx
    ON pending_events(delivered, created_at, execution_id, source_sequence);
CREATE TABLE IF NOT EXISTS desired_actions (
    action_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(execution_id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    action_document BLOB NOT NULL,
    acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0, 1)),
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, revision)
);
CREATE TABLE IF NOT EXISTS log_positions (
    execution_id TEXT NOT NULL REFERENCES executions(execution_id),
    stream TEXT NOT NULL CHECK (stream IN ('stdout', 'stderr')),
    byte_offset INTEGER NOT NULL DEFAULT 0 CHECK (byte_offset >= 0),
    next_sequence INTEGER NOT NULL DEFAULT 1 CHECK (next_sequence > 0),
    complete INTEGER NOT NULL DEFAULT 0 CHECK (complete IN (0, 1)),
    PRIMARY KEY (execution_id, stream)
);
CREATE TABLE IF NOT EXISTS pending_log_chunks (
    execution_id TEXT NOT NULL REFERENCES executions(execution_id),
    stream TEXT NOT NULL CHECK (stream IN ('stdout', 'stderr')),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    chunk_document BLOB NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0 CHECK (delivered IN (0, 1)),
    created_at TEXT NOT NULL,
    PRIMARY KEY (execution_id, stream, sequence)
);
CREATE INDEX IF NOT EXISTS pending_log_chunks_delivery_idx
    ON pending_log_chunks(delivered, created_at, execution_id, stream, sequence
);`
	if _, err := spool.database.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize agent spool: %w", err)
	}

	return nil
}

// logPosition returns the next durable byte offset and sequence for a stream.
func (spool *Spool) logPosition(ctx context.Context, executionID, stream string) (logPosition, error) {
	if stream != logStreamStdout && stream != logStreamStderr {
		return logPosition{}, errors.New("read log position: invalid stream")
	}
	var position logPosition
	var complete int
	err := spool.database.QueryRowContext(ctx, `
SELECT byte_offset, next_sequence, complete FROM log_positions
WHERE execution_id = ? AND stream = ?`, executionID, stream).Scan(
		&position.ByteOffset, &position.NextSequence, &complete,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return logPosition{NextSequence: 1}, nil
	}
	if err != nil {
		return logPosition{}, fmt.Errorf("read log position: %w", err)
	}
	position.Complete = complete == 1

	return position, nil
}

// queueLogChunk journals committed object metadata and advances the local
// capture cursor atomically. Replaying identical metadata is safe.
//
//nolint:cyclop // The transaction checks every replay and cursor invariant before its one commit.
func (spool *Spool) queueLogChunk(ctx context.Context, chunk agentLogChunk) (resultErr error) {
	if err := validateAgentLogChunk(chunk); err != nil {
		return fmt.Errorf("queue log chunk: %w", err)
	}
	document, err := json.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("queue log chunk: %w", err)
	}
	transaction, err := spool.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("queue log chunk: begin: %w", err)
	}
	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, rollbackErr)
		}
	}()
	var existing []byte
	err = transaction.QueryRowContext(ctx, `
SELECT chunk_document FROM pending_log_chunks
WHERE execution_id = ? AND stream = ? AND sequence = ?`,
		chunk.Metadata.ExecutionID, chunk.Metadata.Stream, chunk.Metadata.Sequence,
	).Scan(&existing)
	if err == nil {
		if !bytes.Equal(existing, document) {
			return errSpoolConflict
		}

		return transaction.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("queue log chunk: inspect replay: %w", err)
	}
	position := logPosition{NextSequence: 1}
	var complete int
	err = transaction.QueryRowContext(ctx, `
SELECT byte_offset, next_sequence, complete FROM log_positions
WHERE execution_id = ? AND stream = ?`, chunk.Metadata.ExecutionID, chunk.Metadata.Stream).Scan(
		&position.ByteOffset, &position.NextSequence, &complete,
	)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("queue log chunk: read position: %w", err)
	}
	position.Complete = complete == 1
	if position.Complete || position.ByteOffset != chunk.Spec.ByteOffset || position.NextSequence != chunk.Metadata.Sequence {
		return errSpoolConflict
	}
	if _, err = transaction.ExecContext(ctx, `
INSERT INTO pending_log_chunks (
    execution_id, stream, sequence, chunk_document, created_at
) VALUES (?, ?, ?, ?, ?)`, chunk.Metadata.ExecutionID, chunk.Metadata.Stream,
		chunk.Metadata.Sequence, document, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("queue log chunk: insert: %w", err)
	}
	if _, err = transaction.ExecContext(ctx, `
INSERT INTO log_positions (execution_id, stream, byte_offset, next_sequence, complete)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(execution_id, stream) DO UPDATE SET
    byte_offset = excluded.byte_offset,
    next_sequence = excluded.next_sequence,
    complete = excluded.complete`, chunk.Metadata.ExecutionID, chunk.Metadata.Stream,
		chunk.Spec.ByteOffset+chunk.Spec.ByteLength, chunk.Metadata.Sequence+1, boolInt(chunk.Spec.Complete)); err != nil {
		return fmt.Errorf("queue log chunk: advance position: %w", err)
	}
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("queue log chunk: commit: %w", err)
	}

	return nil
}

// pendingLogChunks returns unpublished metadata in capture order.
func (spool *Spool) pendingLogChunks(ctx context.Context) ([]agentLogChunk, error) {
	rows, err := spool.database.QueryContext(ctx, `
SELECT chunk_document FROM pending_log_chunks
WHERE delivered = 0 ORDER BY created_at, execution_id, sequence, stream`)
	if err != nil {
		return nil, fmt.Errorf("list pending log chunks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var result []agentLogChunk
	for rows.Next() {
		var document []byte
		if err = rows.Scan(&document); err != nil {
			return nil, fmt.Errorf("list pending log chunks: scan: %w", err)
		}
		var chunk agentLogChunk
		if err = decodeStrictJSON(document, &chunk); err != nil {
			return nil, fmt.Errorf("list pending log chunks: decode: %w", err)
		}
		if err = validateAgentLogChunk(chunk); err != nil {
			return nil, fmt.Errorf("list pending log chunks: %w", err)
		}
		result = append(result, chunk)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("list pending log chunks: rows: %w", err)
	}

	return result, nil
}

// markLogChunkDelivered records Control receipt of metadata.
func (spool *Spool) markLogChunkDelivered(ctx context.Context, executionID, stream string, sequence int64) error {
	result, err := spool.database.ExecContext(ctx, `
UPDATE pending_log_chunks SET delivered = 1
WHERE execution_id = ? AND stream = ? AND sequence = ?`, executionID, stream, sequence)
	if err != nil {
		return fmt.Errorf("mark log chunk delivered: %w", err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil || affected != 1 {
		return errors.Join(errSpoolConflict, rowsErr)
	}

	return nil
}

func validateAgentLogChunk(chunk agentLogChunk) error {
	if !validLogChunkEnvelope(chunk) || !validLogChunkSpec(chunk.Spec) {
		return errors.New("invalid log chunk")
	}

	return nil
}

func validLogChunkEnvelope(chunk agentLogChunk) bool {
	return chunk.APIVersion == controlAPIVersion && chunk.Kind == "LogChunk" &&
		validUUID(chunk.Metadata.ExecutionID) &&
		(chunk.Metadata.Stream == logStreamStdout || chunk.Metadata.Stream == logStreamStderr) &&
		chunk.Metadata.Sequence > 0
}

func validLogChunkSpec(spec logChunkSpec) bool {
	return spec.StoreName != "" && spec.StoreVersion > 0 && spec.ObjectKey != "" &&
		spec.ByteOffset >= 0 && spec.ByteLength >= 0 && spec.ByteLength <= logChunkBytes &&
		(spec.ByteLength > 0 || spec.Complete) && artifact.ValidDigest(spec.Checksum) &&
		!spec.CapturedAt.IsZero() && (!spec.Truncated || spec.Complete)
}

func boolInt(value bool) int {
	if value {
		return 1
	}

	return 0
}

// Close releases the spool database handle.
func (spool *Spool) Close() error {
	if spool == nil || spool.database == nil {
		return nil
	}

	return spool.database.Close()
}

// PutAssignment journals an inert assignment before the agent asks to accept
// it. Replaying identical content is safe; changed content is rejected.
func (spool *Spool) PutAssignment(ctx context.Context, assignment protocol.SealedAgentAssignment) (resultErr error) {
	value := assignment.Document
	if err := protocol.ValidateAgentAssignment(value); err != nil {
		return fmt.Errorf("journal assignment: %w", err)
	}
	transaction, err := spool.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("journal assignment: begin: %w", err)
	}
	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, fmt.Errorf("journal assignment: rollback: %w", rollbackErr))
		}
	}()

	var document []byte
	err = transaction.QueryRowContext(
		ctx, "SELECT assignment_document FROM executions WHERE delivery_id = ? OR execution_id = ?",
		value.Metadata.DeliveryID, value.Spec.EffectiveExecution.Metadata.ExecutionID,
	).Scan(&document)
	if err == nil {
		if !bytes.Equal(document, assignment.CanonicalJSON) {
			return errSpoolConflict
		}

		return transaction.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("journal assignment: inspect replay: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = transaction.ExecContext(ctx, `
INSERT INTO executions (
    execution_id, delivery_id, assignment_document, assignment_digest, state, created_at, updated_at
) VALUES (?, ?, ?, ?, 'offered', ?, ?)`,
		value.Spec.EffectiveExecution.Metadata.ExecutionID, value.Metadata.DeliveryID,
		assignment.CanonicalJSON, assignment.EffectiveExecutionDigest, now, now,
	)
	if err != nil {
		return fmt.Errorf("journal assignment: insert: %w", err)
	}
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("journal assignment: commit: %w", err)
	}

	return nil
}

// RecordAcceptance stores the server's launch authorization before any target
// process is created. An identical replay is safe.
func (spool *Spool) RecordAcceptance(
	ctx context.Context,
	executionID string,
	authorization protocol.LaunchAuthorization,
) (resultErr error) {
	if err := protocol.ValidateLaunchAuthorization(authorization); err != nil {
		return fmt.Errorf("record assignment acceptance: %w", err)
	}
	if authorization.Metadata.ExecutionID != executionID {
		return errors.New("record assignment acceptance: execution identity mismatch")
	}
	encoded, err := marshalStrictJSON(authorization)
	if err != nil {
		return fmt.Errorf("record assignment acceptance: %w", err)
	}
	transaction, err := spool.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("record assignment acceptance: begin: %w", err)
	}
	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, fmt.Errorf("record assignment acceptance: rollback: %w", rollbackErr))
		}
	}()
	var state string
	var existing []byte
	if err = transaction.QueryRowContext(ctx, `
SELECT state, authorization_document FROM executions WHERE execution_id = ?`, executionID).Scan(&state, &existing); err != nil {
		return fmt.Errorf("record assignment acceptance: find execution: %w", err)
	}
	if state != "offered" {
		if !bytes.Equal(existing, encoded) {
			return errSpoolConflict
		}

		return transaction.Commit()
	}
	result, err := transaction.ExecContext(ctx, `
UPDATE executions SET authorization_document = ?, state = 'accepted', updated_at = ?
WHERE execution_id = ? AND state = 'offered'`, encoded, time.Now().UTC().Format(time.RFC3339Nano), executionID)
	if err != nil {
		return fmt.Errorf("record assignment acceptance: update: %w", err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil || affected != 1 {
		return errors.Join(errSpoolConflict, rowsErr)
	}
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("record assignment acceptance: commit: %w", err)
	}

	return nil
}

// SetExecutionState advances a locally accepted execution. State transitions
// are monotonic; terminal state is idempotent.
func (spool *Spool) SetExecutionState(ctx context.Context, executionID, state string) error {
	ranks := map[string]int{"accepted": 1, "launching": 2, "running": 3, "terminal": 4}
	wantedRank, valid := ranks[state]
	if !valid {
		return fmt.Errorf("set execution state: invalid state %q", state)
	}
	var current string
	if err := spool.database.QueryRowContext(ctx, "SELECT state FROM executions WHERE execution_id = ?", executionID).Scan(&current); err != nil {
		return fmt.Errorf("set execution state: %w", err)
	}
	currentRank, valid := ranks[current]
	if !valid || currentRank > wantedRank {
		return errSpoolConflict
	}
	if current == state {
		return nil
	}
	result, err := spool.database.ExecContext(ctx, `
UPDATE executions SET state = ?, updated_at = ? WHERE execution_id = ? AND state = ?`,
		state, time.Now().UTC().Format(time.RFC3339Nano), executionID, current,
	)
	if err != nil {
		return fmt.Errorf("set execution state: %w", err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil || affected != 1 {
		return errors.Join(errSpoolConflict, rowsErr)
	}

	return nil
}

// ListExecutions returns all locally accepted executions that may require
// launch, monitoring, or event delivery.
func (spool *Spool) ListExecutions(ctx context.Context) ([]SpoolExecution, error) {
	rows, err := spool.database.QueryContext(ctx, `
SELECT assignment_document, authorization_document, state
FROM executions WHERE state <> 'offered' ORDER BY created_at, execution_id`)
	if err != nil {
		return nil, fmt.Errorf("list agent executions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var result []SpoolExecution
	for rows.Next() {
		var assignmentDocument []byte
		var authorizationDocument []byte
		var execution SpoolExecution
		if err = rows.Scan(&assignmentDocument, &authorizationDocument, &execution.State); err != nil {
			return nil, fmt.Errorf("list agent executions: scan: %w", err)
		}
		execution.Assignment, err = protocol.DecodeAgentAssignment(bytes.NewReader(assignmentDocument), protocol.DecodeLimits{})
		if err != nil {
			return nil, fmt.Errorf("list agent executions: decode assignment: %w", err)
		}
		var authorization protocol.LaunchAuthorization
		if err = decodeStrictJSON(authorizationDocument, &authorization); err != nil {
			return nil, fmt.Errorf("list agent executions: decode authorization: %w", err)
		}
		if err = protocol.ValidateLaunchAuthorization(authorization); err != nil {
			return nil, fmt.Errorf("list agent executions: %w", err)
		}
		execution.Authorization = &authorization
		result = append(result, execution)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("list agent executions: rows: %w", err)
	}

	return result, nil
}

// QueueEvent durably records an event for replay until the server confirms it.
func (spool *Spool) QueueEvent(ctx context.Context, event protocol.SealedExecutionEvent) error {
	value := event.Document
	result, err := spool.database.ExecContext(ctx, `
INSERT INTO pending_events (
    event_id, execution_id, source_sequence, event_document, event_digest, created_at
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(event_id) DO NOTHING`, value.Metadata.EventID, value.Metadata.ExecutionID,
		value.Metadata.Sequence, event.CanonicalJSON, event.Digest, time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("queue execution event: %w", err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("queue execution event: %w", rowsErr)
	} else if affected == 0 {
		var existing []byte
		if err = spool.database.QueryRowContext(ctx, "SELECT event_document FROM pending_events WHERE event_id = ?", value.Metadata.EventID).Scan(&existing); err != nil {
			return fmt.Errorf("queue execution event: inspect replay: %w", err)
		}
		if !bytes.Equal(existing, event.CanonicalJSON) {
			return errSpoolConflict
		}
	}

	return nil
}

// EventExists reports whether an event identity is already in the durable
// local stream, whether pending or delivered.
func (spool *Spool) EventExists(ctx context.Context, eventID string) (bool, error) {
	var exists int
	if err := spool.database.QueryRowContext(
		ctx, "SELECT EXISTS(SELECT 1 FROM pending_events WHERE event_id = ?)", eventID,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("inspect execution event: %w", err)
	}

	return exists == 1, nil
}

// NextEventSequence returns the next monotonic source sequence from the
// durable local stream.
func (spool *Spool) NextEventSequence(ctx context.Context, executionID string) (int64, error) {
	var sequence int64
	if err := spool.database.QueryRowContext(ctx, `
SELECT COALESCE(MAX(source_sequence), 0) + 1 FROM pending_events
WHERE execution_id = ?`, executionID).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("read next execution event sequence: %w", err)
	}

	return sequence, nil
}

// PendingEvents returns unsent events in per-execution source order.
func (spool *Spool) PendingEvents(ctx context.Context) ([]PendingEvent, error) {
	rows, err := spool.database.QueryContext(ctx, `
SELECT event_document FROM pending_events
WHERE delivered = 0 ORDER BY created_at, execution_id, source_sequence`)
	if err != nil {
		return nil, fmt.Errorf("list pending execution events: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var result []PendingEvent
	for rows.Next() {
		var document []byte
		if err = rows.Scan(&document); err != nil {
			return nil, fmt.Errorf("list pending execution events: scan: %w", err)
		}
		event, decodeErr := protocol.DecodeExecutionEvent(bytes.NewReader(document), protocol.DecodeLimits{})
		if decodeErr != nil {
			return nil, fmt.Errorf("list pending execution events: decode: %w", decodeErr)
		}
		result = append(result, PendingEvent{Event: event})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("list pending execution events: rows: %w", err)
	}

	return result, nil
}

// MarkEventDelivered records a successful control-plane receipt.
func (spool *Spool) MarkEventDelivered(ctx context.Context, eventID string) error {
	result, err := spool.database.ExecContext(ctx, "UPDATE pending_events SET delivered = 1 WHERE event_id = ?", eventID)
	if err != nil {
		return fmt.Errorf("mark execution event delivered: %w", err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil || affected != 1 {
		return errors.Join(errSpoolConflict, rowsErr)
	}

	return nil
}

// PutAction journals control intent before the corresponding native marker is
// created or an acknowledgement is sent.
func (spool *Spool) PutAction(ctx context.Context, action protocol.DesiredAction) error {
	if err := protocol.ValidateDesiredAction(action); err != nil {
		return fmt.Errorf("journal desired action: %w", err)
	}
	document, err := json.Marshal(action)
	if err != nil {
		return fmt.Errorf("journal desired action: %w", err)
	}
	result, err := spool.database.ExecContext(ctx, `
INSERT INTO desired_actions (
    action_id, execution_id, revision, action_document, created_at
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(action_id) DO NOTHING`, action.Metadata.ActionID, action.Metadata.ExecutionID,
		action.Metadata.Revision, document, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("journal desired action: %w", err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("journal desired action: %w", rowsErr)
	} else if affected == 0 {
		var existing []byte
		if err = spool.database.QueryRowContext(ctx, "SELECT action_document FROM desired_actions WHERE action_id = ?", action.Metadata.ActionID).Scan(&existing); err != nil {
			return fmt.Errorf("journal desired action: inspect replay: %w", err)
		}
		if !bytes.Equal(existing, document) {
			return errSpoolConflict
		}
	}

	return nil
}

// MarkActionAcknowledged records the successful server acknowledgement.
func (spool *Spool) MarkActionAcknowledged(ctx context.Context, actionID string) error {
	result, err := spool.database.ExecContext(ctx, `
UPDATE desired_actions SET acknowledged = 1 WHERE action_id = ?`, actionID)
	if err != nil {
		return fmt.Errorf("mark desired action acknowledged: %w", err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil || affected != 1 {
		return errors.Join(errSpoolConflict, rowsErr)
	}

	return nil
}

func marshalStrictJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}

func decodeStrictJSON(document []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("additional JSON value")
		}

		return err
	}

	return nil
}
