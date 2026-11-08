package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const recoveryFilename = "credential-recovery.json"

type recoveryRequest struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		AgentID string `json:"agentId"`
	} `json:"metadata"`
	Spec struct {
		TargetGenerationID        string `json:"targetGenerationId"`
		PreviousCertificateSerial string `json:"previousCertificateSerial"`
		CertificateSigningRequest string `json:"certificateSigningRequest"`
		ExecutionUser             string `json:"executionUser"`
	} `json:"spec"`
}

type pendingRecovery struct {
	ServerURL                  string          `json:"serverUrl"`
	TokenHash                  string          `json:"tokenHash"`
	PrivateKeyPEM              []byte          `json:"privateKeyPem"`
	Request                    recoveryRequest `json:"request"`
	CompletedCertificateSerial string          `json:"completedCertificateSerial,omitempty"`
}

// Recover exchanges a fresh operator-issued enrollment token for credentials
// belonging to the existing agent identity. The service must be stopped. Only
// private credential files change; execution journals and manifests are retained.
func Recover(ctx context.Context, stateDirectory, token, executionUser string) error {
	if ctx == nil {
		return errors.New("recover agent: nil context")
	}
	if token == "" || strings.ContainsAny(token, " \t\r\n") || executionUser == "" {
		return errors.New("recover agent: enrollment token and execution user are required")
	}
	directory, err := prepareStateDirectory(stateDirectory)
	if err != nil {
		return fmt.Errorf("recover agent: %w", err)
	}
	lock, err := lockState(directory)
	if err != nil {
		return fmt.Errorf("recover agent: %w", err)
	}
	defer func() { _ = lock.Close() }()
	credentials, err := loadCredentials(directory)
	if err != nil {
		return fmt.Errorf("recover agent: %w", err)
	}
	pending, completed, err := prepareRecovery(directory, credentials, token, executionUser)
	if err != nil {
		return fmt.Errorf("recover agent: %w", err)
	}
	if completed {
		return nil
	}
	baseURL, err := parseServerURL(pending.ServerURL)
	if err != nil {
		return fmt.Errorf("recover agent: %w", err)
	}
	// Recovery authenticates the server and the new enrollment grant. Never send
	// an expired client certificate or disable TLS certificate validation.
	client, err := newTLSHTTPClient(credentials.ServerCAPEM, nil)
	if err != nil {
		return fmt.Errorf("recover agent: %w", err)
	}
	defer client.CloseIdleConnections()
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	var response sessionResponse
	if err = performJSON(ctx, client, baseURL, http.MethodPost, "/v1/agent/recover", "Jobman-Enrollment "+token, pending.Request, &response); err != nil {
		return fmt.Errorf("recover agent: %w; recovery request retained, retry with the same enrollment token; if rejected again, preserve state for Control operator inspection (expired replay, revocation, or generation change may require intervention)", err)
	}
	updated, err := recoveredCredentials(credentials, pending, response)
	if err != nil {
		return fmt.Errorf("recover agent: %w", err)
	}
	if err = saveCredentials(directory, updated); err != nil {
		return fmt.Errorf("recover agent: save credentials: %w", err)
	}
	return completeRecovery(directory, &pending, updated)
}

func readRecovery(directory string) (pendingRecovery, error) {
	var pending pendingRecovery
	path := filepath.Join(directory, recoveryFilename)
	information, err := os.Lstat(path)
	if err != nil {
		return pending, err
	}
	if !information.Mode().IsRegular() || information.Size() > 65536 || !privateFileMode(information.Mode()) {
		return pending, errors.New("credential recovery receipt must be a bounded regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return pending, err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(information, opened) {
		return pending, errors.New("credential recovery receipt changed while opening")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, 65537))
	if err != nil {
		return pending, err
	}
	if len(encoded) > 65536 {
		return pending, errors.New("credential recovery receipt exceeds the size limit")
	}
	if err := decodeStrictJSON(encoded, &pending); err != nil {
		return pending, err
	}
	return pending, nil
}

func writeRecovery(directory string, pending pendingRecovery) error {
	encoded, err := json.MarshalIndent(pending, "", "  ")
	if err != nil {
		return err
	}
	return writePrivateFile(filepath.Join(directory, recoveryFilename), append(encoded, '\n'))
}

func prepareRecovery(directory string, credentials credentialFiles, token, executionUser string) (pendingRecovery, bool, error) {
	tokenHash := fmt.Sprintf("%x", sha256.Sum256([]byte(token)))
	pending, err := readRecovery(directory)
	if errors.Is(err, fs.ErrNotExist) {
		return newRecovery(directory, credentials, tokenHash, executionUser)
	}
	if err != nil {
		return pending, false, fmt.Errorf("read recovery receipt: %w", err)
	}
	if pending.ServerURL != credentials.Metadata.ServerURL || pending.Request.Metadata.AgentID != credentials.Metadata.AgentID ||
		pending.Request.Spec.TargetGenerationID != credentials.Metadata.TargetGenerationID || pending.Request.Spec.ExecutionUser != executionUser {
		return pending, false, errors.New("pending credential recovery belongs to a different agent, target, server, or execution user")
	}
	if pending.CompletedCertificateSerial == "" && bytes.Equal(credentials.PrivateKeyPEM, pending.PrivateKeyPEM) {
		if err := completeRecovery(directory, &pending, credentials); err != nil {
			return pending, false, err
		}
	}
	if pending.CompletedCertificateSerial == "" {
		return resumeRecovery(pending, credentials, tokenHash)
	}
	if pending.TokenHash != tokenHash {
		// A fresh explicit grant may replace only a completed operation.
		return newRecovery(directory, credentials, tokenHash, executionUser)
	}
	if !credentials.Metadata.CertificateExpiresAt.After(time.Now()) {
		return pending, false, errors.New("previous credential recovery completed; obtain a fresh enrollment token for the new expiry")
	}
	return pending, true, nil
}

func resumeRecovery(pending pendingRecovery, credentials credentialFiles, tokenHash string) (pendingRecovery, bool, error) {
	if pending.TokenHash != tokenHash {
		return pending, false, errors.New("credential recovery is pending; retry with its original enrollment token")
	}
	certificate, err := credentialCertificate(credentials)
	if err != nil || certificate.SerialNumber.Text(16) != pending.Request.Spec.PreviousCertificateSerial {
		return pending, false, errors.New("credentials changed during pending recovery; preserve state for operator inspection")
	}
	return pending, false, nil
}

func newRecovery(directory string, credentials credentialFiles, tokenHash, executionUser string) (pendingRecovery, bool, error) {
	var pending pendingRecovery
	certificate, err := credentialCertificate(credentials)
	if err != nil {
		return pending, false, err
	}
	if certificate.NotAfter.After(time.Now()) {
		return pending, false, errors.New("agent certificate is still valid; start the service to renew its session")
	}
	key, csr, err := generateKeyAndCSR(credentials.Metadata.AgentID)
	if err != nil {
		return pending, false, err
	}
	pending = pendingRecovery{
		ServerURL: credentials.Metadata.ServerURL, TokenHash: tokenHash, PrivateKeyPEM: key,
		Request: recoveryRequest{APIVersion: controlAPIVersion, Kind: "AgentRecovery"},
	}
	pending.Request.Metadata.AgentID = credentials.Metadata.AgentID
	pending.Request.Spec.TargetGenerationID = credentials.Metadata.TargetGenerationID
	pending.Request.Spec.PreviousCertificateSerial = certificate.SerialNumber.Text(16)
	pending.Request.Spec.CertificateSigningRequest = csr
	pending.Request.Spec.ExecutionUser = executionUser
	if err = writeRecovery(directory, pending); err != nil {
		return pending, false, fmt.Errorf("persist recovery request: %w", err)
	}
	return pending, false, nil
}

func recoveredCredentials(current credentialFiles, pending pendingRecovery, response sessionResponse) (credentialFiles, error) {
	if response.APIVersion != controlAPIVersion || response.Kind != agentSessionKind || response.Metadata.AgentID != current.Metadata.AgentID ||
		response.Metadata.SessionID == "" || !response.Metadata.ExpiresAt.After(time.Now()) || response.Spec.Token == "" ||
		response.Spec.AuthScheme != agentAuthScheme || response.Spec.InertOnly || !response.Spec.Certificate.ExpiresAt.After(time.Now()) {
		return credentialFiles{}, errors.New("control server returned an invalid recovered agent identity or session")
	}
	updated := current
	updated.Metadata.SessionID = response.Metadata.SessionID
	updated.Metadata.SessionToken = response.Spec.Token
	updated.Metadata.SessionExpiresAt = response.Metadata.ExpiresAt.UTC()
	updated.Metadata.CertificateExpiresAt = response.Spec.Certificate.ExpiresAt.UTC()
	updated.PrivateKeyPEM = pending.PrivateKeyPEM
	updated.CertificatePEM = []byte(response.Spec.Certificate.CertificatePEM)
	if err := validateRecoveredCertificate(updated, pending, response.Spec.Certificate); err != nil {
		return credentialFiles{}, err
	}
	return updated, nil
}

func validateRecoveredCertificate(updated credentialFiles, pending pendingRecovery, response certificateResponse) error {
	certificate, err := credentialCertificate(updated)
	if err != nil {
		return err
	}
	if len(certificate.URIs) != 1 || certificate.URIs[0].String() != "urn:jobman:agent:"+updated.Metadata.AgentID ||
		certificate.SerialNumber.Text(16) != response.Serial ||
		certificate.SerialNumber.Text(16) == pending.Request.Spec.PreviousCertificateSerial ||
		!certificate.NotAfter.After(time.Now()) || !certificate.NotAfter.Equal(response.ExpiresAt.Truncate(time.Second)) {
		return errors.New("control server returned an invalid recovered agent certificate")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(response.CACertificatePEM)) {
		return errors.New("control server returned an invalid recovered agent CA")
	}
	if _, err = certificate.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return fmt.Errorf("verify recovered agent certificate: %w", err)
	}
	return nil
}

func credentialCertificate(credentials credentialFiles) (*x509.Certificate, error) {
	pair, err := tls.X509KeyPair(credentials.CertificatePEM, credentials.PrivateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("validate agent certificate key pair: %w", err)
	}
	certificate, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse agent certificate: %w", err)
	}
	return certificate, nil
}

func completeRecovery(directory string, pending *pendingRecovery, credentials credentialFiles) error {
	certificate, err := credentialCertificate(credentials)
	if err != nil {
		return err
	}
	if !bytes.Equal(credentials.PrivateKeyPEM, pending.PrivateKeyPEM) ||
		certificate.SerialNumber.Text(16) == pending.Request.Spec.PreviousCertificateSerial {
		return errors.New("installed credentials do not match pending recovery")
	}
	pending.CompletedCertificateSerial = certificate.SerialNumber.Text(16)
	if err = writeRecovery(directory, *pending); err != nil {
		return fmt.Errorf("record completed credential recovery: %w", err)
	}
	return nil
}

func checkRecoveryBeforeService(directory string, credentials credentialFiles) error {
	pending, err := readRecovery(directory)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect credential recovery: %w", err)
	}
	if pending.CompletedCertificateSerial != "" {
		return nil
	}
	if pending.ServerURL != credentials.Metadata.ServerURL || pending.Request.Metadata.AgentID != credentials.Metadata.AgentID ||
		pending.Request.Spec.TargetGenerationID != credentials.Metadata.TargetGenerationID || !bytes.Equal(credentials.PrivateKeyPEM, pending.PrivateKeyPEM) {
		return errors.New("credential recovery is pending; stop the service and retry recover with the original enrollment token")
	}
	return completeRecovery(directory, &pending, credentials)
}
