package jobman

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"github.com/ryancswallace/jobman/protocol"
)

const (
	sharedOperationVersion   = 1
	sharedOperationPending   = "pending"
	sharedOperationCompleted = "completed"
)

var sharedOperationIDPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
)

type sharedOperation struct {
	Version        int             `json:"version"`
	ID             string          `json:"id"`
	Profile        string          `json:"profile"`
	Namespace      string          `json:"namespace"`
	IdempotencyKey string          `json:"idempotencyKey"`
	RequestDigest  string          `json:"requestDigest"`
	Request        json.RawMessage `json:"request"`
	Status         string          `json:"status"`
	JobID          string          `json:"jobId,omitempty"`
	CreatedAt      time.Time       `json:"createdAt"`
	UpdatedAt      time.Time       `json:"updatedAt"`
}

type sharedOperationSummary struct {
	ID        string    `json:"id"`
	Profile   string    `json:"profile"`
	Namespace string    `json:"namespace"`
	Status    string    `json:"status"`
	JobID     string    `json:"job_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func createSharedOperation(
	stateDir,
	profile,
	namespace string,
	request protocol.SealedJobRequest,
) (sharedOperation, error) {
	id, err := newSharedOperationID()
	if err != nil {
		return sharedOperation{}, err
	}
	now := time.Now().UTC()
	operation := sharedOperation{
		Version: sharedOperationVersion, ID: id, Profile: profile, Namespace: namespace,
		IdempotencyKey: "submit-" + id, RequestDigest: request.RequestDigest,
		Request: append(json.RawMessage(nil), request.CanonicalJSON...),
		Status:  sharedOperationPending, CreatedAt: now, UpdatedAt: now,
	}
	if writeErr := writeSharedOperation(stateDir, operation); writeErr != nil {
		return sharedOperation{}, writeErr
	}

	return operation, nil
}

func completeSharedOperation(stateDir string, operation *sharedOperation, jobID string) error {
	operation.Status = sharedOperationCompleted
	operation.JobID = jobID
	operation.UpdatedAt = time.Now().UTC()

	return writeSharedOperation(stateDir, *operation)
}

func loadSharedOperation(stateDir, id string) (sharedOperation, error) {
	filePath, err := sharedOperationPath(stateDir, id)
	if err != nil {
		return sharedOperation{}, err
	}
	file, err := os.Open(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return sharedOperation{}, fmt.Errorf("shared submission operation %q was not found", id)
		}

		return sharedOperation{}, fmt.Errorf("open shared submission operation: %w", err)
	}
	encoded, readErr := io.ReadAll(io.LimitReader(file, 2*1024*1024+1))
	closeErr := file.Close()
	if readErr != nil {
		return sharedOperation{}, fmt.Errorf("read shared submission operation: %w", readErr)
	}
	if closeErr != nil {
		return sharedOperation{}, fmt.Errorf("close shared submission operation: %w", closeErr)
	}
	if len(encoded) > 2*1024*1024 {
		return sharedOperation{}, errors.New("shared submission operation exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var operation sharedOperation
	if err = decoder.Decode(&operation); err != nil {
		return sharedOperation{}, fmt.Errorf("decode shared submission operation: %w", err)
	}
	if err = decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return sharedOperation{}, errors.New("shared submission operation contains trailing data")
	}
	if validationErr := validateSharedOperation(operation, id); validationErr != nil {
		return sharedOperation{}, validationErr
	}

	return operation, nil
}

func listSharedOperations(stateDir string) ([]sharedOperationSummary, error) {
	directory := sharedOperationsDirectory(stateDir)
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return []sharedOperationSummary{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list shared submission operations: %w", err)
	}
	operations := make([]sharedOperationSummary, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		id := entry.Name()[:len(entry.Name())-len(".json")]
		operation, loadErr := loadSharedOperation(stateDir, id)
		if loadErr != nil {
			return nil, loadErr
		}
		operations = append(operations, sharedOperationSummary{
			ID: operation.ID, Profile: operation.Profile, Namespace: operation.Namespace,
			Status: operation.Status, JobID: operation.JobID,
			CreatedAt: operation.CreatedAt, UpdatedAt: operation.UpdatedAt,
		})
	}
	sort.Slice(operations, func(left, right int) bool {
		return operations[left].CreatedAt.After(operations[right].CreatedAt)
	})

	return operations, nil
}

func validateSharedOperation(operation sharedOperation, expectedID string) error {
	if operation.Version != sharedOperationVersion || operation.ID != expectedID ||
		!sharedOperationIDPattern.MatchString(operation.ID) || operation.Profile == "" ||
		operation.Namespace == "" || operation.IdempotencyKey != "submit-"+operation.ID ||
		operation.RequestDigest == "" || operation.CreatedAt.IsZero() || operation.UpdatedAt.IsZero() {
		return errors.New("shared submission operation is invalid")
	}
	if operation.Status != sharedOperationPending && operation.Status != sharedOperationCompleted {
		return errors.New("shared submission operation has invalid status")
	}
	if (operation.Status == sharedOperationCompleted) != (operation.JobID != "") {
		return errors.New("shared submission operation has inconsistent completion")
	}
	sealed, err := protocol.DecodeJobRequest(bytes.NewReader(operation.Request), protocol.DecodeLimits{})
	if err != nil || sealed.RequestDigest != operation.RequestDigest {
		return errors.New("shared submission operation request is invalid")
	}

	return nil
}

func writeSharedOperation(stateDir string, operation sharedOperation) error {
	directory := sharedOperationsDirectory(stateDir)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create shared submission operation directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil { //nolint:gosec // Directories require user traversal.
		return fmt.Errorf("protect shared submission operation directory: %w", err)
	}
	filePath, err := sharedOperationPath(stateDir, operation.ID)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(operation)
	if err != nil {
		return fmt.Errorf("encode shared submission operation: %w", err)
	}
	file, err := os.CreateTemp(directory, operation.ID+".*.tmp")
	if err != nil {
		return fmt.Errorf("create shared submission operation update: %w", err)
	}
	temporary := file.Name()
	writeErr := writeSyncClose(file, append(encoded, '\n'))
	if writeErr != nil {
		_ = os.Remove(temporary)
		return writeErr
	}
	if err = os.Rename(temporary, filePath); err != nil {
		_ = os.Remove(temporary)
		return fmt.Errorf("commit shared submission operation: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open shared submission operation directory: %w", err)
	}
	syncErr := directoryHandle.Sync()
	closeErr := directoryHandle.Close()
	if syncErr != nil {
		return errors.Join(fmt.Errorf("sync shared submission operation directory: %w", syncErr), closeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close shared submission operation directory: %w", closeErr)
	}

	return nil
}

func writeSyncClose(file *os.File, encoded []byte) error {
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		return fmt.Errorf("write shared submission operation: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync shared submission operation: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close shared submission operation: %w", err)
	}

	return nil
}

func sharedOperationPath(stateDir, id string) (string, error) {
	if !sharedOperationIDPattern.MatchString(id) {
		return "", errors.New("shared submission operation ID is invalid")
	}

	return filepath.Join(sharedOperationsDirectory(stateDir), id+".json"), nil
}

func sharedOperationsDirectory(stateDir string) string {
	return filepath.Join(stateDir, "shared", "operations")
}

func newSharedOperationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate shared submission operation ID: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	var encoded [36]byte
	hex.Encode(encoded[0:8], raw[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], raw[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], raw[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], raw[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], raw[10:16])

	return string(encoded[:]), nil
}
