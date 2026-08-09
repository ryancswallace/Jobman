package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
)

const (
	fingerprintSecretName = "failure_fingerprint_hmac_v1"
	fingerprintKeyBytes   = 32
)

func (s *Store) ensureFingerprintKey(ctx context.Context) error {
	key, err := readFingerprintKey(ctx, s.db)
	if err == nil {
		s.fingerprintKey = key

		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var indexed int
	if queryErr := s.db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM run_diagnostic_facts WHERE fingerprint_value IS NOT NULL
		)`).Scan(&indexed); queryErr != nil {
		return fmt.Errorf("inspect missing failure fingerprint key: %w", queryErr)
	}
	if indexed != 0 {
		return &SchemaError{Reason: "failure fingerprint key is missing while indexed fingerprints exist"}
	}

	var generated [fingerprintKeyBytes]byte
	if _, readErr := io.ReadFull(s.random, generated[:]); readErr != nil {
		return fmt.Errorf("initialize failure fingerprint key: read randomness: %w", readErr)
	}
	if writeErr := s.writeTransaction(ctx, "initialize failure fingerprint key", func(tx *sql.Tx) error {
		_, insertErr := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO store_secrets(name, value, created_at_ns)
			VALUES (?, ?, ?)`, fingerprintSecretName, generated[:], s.now().UTC().UnixNano())

		return insertErr
	}); writeErr != nil {
		return writeErr
	}

	key, err = readFingerprintKey(ctx, s.db)
	if err != nil {
		return err
	}
	s.fingerprintKey = key

	return nil
}

func readFingerprintKey(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
},
) ([fingerprintKeyBytes]byte, error) {
	var encoded []byte
	if err := queryer.QueryRowContext(ctx, `
		SELECT value FROM store_secrets WHERE name = ?`, fingerprintSecretName).Scan(&encoded); err != nil {
		return [fingerprintKeyBytes]byte{}, err
	}
	if len(encoded) != fingerprintKeyBytes {
		return [fingerprintKeyBytes]byte{}, &SchemaError{Reason: "failure fingerprint key has an invalid length"}
	}
	var key [fingerprintKeyBytes]byte
	copy(key[:], encoded)

	return key, nil
}

func (s *Store) verifyFingerprintKey(ctx context.Context) error {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM store_secrets`).Scan(&count); err != nil {
		return fmt.Errorf("verify failure fingerprint key: %w", err)
	}
	if count != 1 {
		return &SchemaError{Reason: fmt.Sprintf("store contains %d fingerprint keys, want exactly one", count)}
	}
	key, err := readFingerprintKey(ctx, s.db)
	if err != nil {
		return fmt.Errorf("verify failure fingerprint key: %w", err)
	}
	if key != s.fingerprintKey {
		return &SchemaError{Reason: "loaded failure fingerprint key does not match the store"}
	}

	return nil
}
