package agent

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const (
	agentCredentialsFilename = "credentials.json"
)

type metadata struct {
	ServerURL            string    `json:"serverUrl"`
	AgentID              string    `json:"agentId"`
	TargetGenerationID   string    `json:"targetGenerationId"`
	SessionID            string    `json:"sessionId"`
	SessionToken         string    `json:"sessionToken"`
	SessionExpiresAt     time.Time `json:"sessionExpiresAt"`
	CertificateExpiresAt time.Time `json:"certificateExpiresAt"`
}

type credentialFiles struct {
	Metadata       metadata `json:"metadata"`
	PrivateKeyPEM  []byte   `json:"privateKeyPem"`
	CertificatePEM []byte   `json:"certificatePem"`
	ServerCAPEM    []byte   `json:"serverCaPem,omitempty"`
}

func generateKeyAndCSR(commonName string) (keyPEM []byte, requestPEM string, resultErr error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", fmt.Errorf("generate agent private key: %w", err)
	}
	encodedKey, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, "", fmt.Errorf("encode agent private key: %w", err)
	}
	request, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: commonName},
	}, privateKey)
	if err != nil {
		return nil, "", fmt.Errorf("create agent certificate request: %w", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encodedKey}),
		string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: request})), nil
}

func saveCredentials(stateDirectory string, values credentialFiles) error {
	encoded, err := json.MarshalIndent(values, "", "  ")
	if err != nil {
		return fmt.Errorf("encode agent credentials: %w", err)
	}
	if err = writePrivateFile(
		filepath.Join(stateDirectory, agentCredentialsFilename), append(encoded, '\n'),
	); err != nil {
		return fmt.Errorf("save agent credentials: %w", err)
	}

	return nil
}

func loadCredentials(stateDirectory string) (credentialFiles, error) {
	var result credentialFiles
	encoded, err := os.ReadFile(filepath.Join(stateDirectory, agentCredentialsFilename))
	if err != nil {
		return result, fmt.Errorf("read agent credentials: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&result); err != nil {
		return result, fmt.Errorf("decode agent credentials: %w", err)
	}
	if result.Metadata.AgentID == "" || result.Metadata.ServerURL == "" ||
		result.Metadata.TargetGenerationID == "" || result.Metadata.CertificateExpiresAt.IsZero() ||
		result.Metadata.SessionID == "" || result.Metadata.SessionToken == "" ||
		result.Metadata.SessionExpiresAt.IsZero() ||
		len(result.PrivateKeyPEM) == 0 || len(result.CertificatePEM) == 0 {
		return result, errors.New("agent credentials are incomplete")
	}

	return result, nil
}

func newMTLSHTTPClient(values credentialFiles) (*http.Client, error) {
	certificate, err := tls.X509KeyPair(values.CertificatePEM, values.PrivateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("load agent key pair: %w", err)
	}

	return newTLSHTTPClient(values.ServerCAPEM, &certificate)
}

func newTLSHTTPClient(serverCAPEM []byte, certificate *tls.Certificate) (*http.Client, error) {
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("load system certificate pool: %w", err)
	}
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}
	if len(serverCAPEM) != 0 && !rootCAs.AppendCertsFromPEM(serverCAPEM) {
		return nil, errors.New("control server CA file contains no certificates")
	}
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("default HTTP transport has an unsupported type")
	}
	transport := defaultTransport.Clone()
	configuration := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: rootCAs}
	if certificate != nil {
		configuration.Certificates = []tls.Certificate{*certificate}
	}
	transport.TLSClientConfig = configuration

	return &http.Client{Transport: transport, Timeout: 30 * time.Second}, nil
}
