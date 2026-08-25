package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ryancswallace/jobman/protocol"
)

const (
	controlAPIVersion         = "jobman.control/v1alpha1"
	maximumResponseSize       = 4 << 20
	pendingEnrollmentFilename = "pending-enrollment.json"
)

// Enrollment describes the immutable host facts bound to one enrollment
// token. The token itself is provided separately and is never persisted.
type Enrollment struct {
	ServerURL          string
	TargetGenerationID string
	AgentVersion       string
	OperatingSystem    string
	Architecture       string
	Hostname           string
	ExecutionUser      string
	ExecutionBackends  []string
	Runtimes           []string
	Capabilities       []string
	ServerCAPEM        []byte
}

type enrollmentRequest struct {
	APIVersion string                `json:"apiVersion"`
	Kind       string                `json:"kind"`
	Spec       enrollmentRequestSpec `json:"spec"`
}

type enrollmentRequestSpec struct {
	TargetGenerationID        string         `json:"targetGenerationId"`
	AgentVersion              string         `json:"agentVersion"`
	ProtocolVersions          []string       `json:"protocolVersions"`
	Host                      enrollmentHost `json:"host"`
	ExecutionUser             string         `json:"executionUser"`
	ExecutionBackends         []string       `json:"executionBackends"`
	Runtimes                  []string       `json:"runtimes"`
	Capabilities              []string       `json:"capabilities"`
	CertificateSigningRequest string         `json:"certificateSigningRequest"`
}

type enrollmentHost struct {
	OperatingSystem string `json:"operatingSystem"`
	Architecture    string `json:"architecture"`
	Hostname        string `json:"hostname"`
}

type sessionResponse struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		AgentID   string    `json:"agentId"`
		SessionID string    `json:"sessionId"`
		ExpiresAt time.Time `json:"expiresAt"`
	} `json:"metadata"`
	Spec struct {
		Token       string              `json:"token"`
		AuthScheme  string              `json:"authScheme"`
		InertOnly   bool                `json:"inertOnly"`
		Certificate certificateResponse `json:"certificate"`
	} `json:"spec"`
}

type certificateResponse struct {
	CertificatePEM   string    `json:"certificatePem"`
	CACertificatePEM string    `json:"caCertificatePem"`
	Serial           string    `json:"serial"`
	ExpiresAt        time.Time `json:"expiresAt"`
}

type pendingEnrollment struct {
	ServerURL          string            `json:"serverUrl"`
	TargetGenerationID string            `json:"targetGenerationId"`
	PrivateKeyPEM      []byte            `json:"privateKeyPem"`
	ServerCAPEM        []byte            `json:"serverCaPem,omitempty"`
	Request            enrollmentRequest `json:"request"`
}

// Enroll exchanges a one-time token and fresh public key for a short-lived
// agent certificate, then atomically persists the resulting credentials.
//
//nolint:cyclop // Enrollment is a linear durability pipeline with distinct failure context at each boundary.
func Enroll(ctx context.Context, stateDirectory, token string, values Enrollment) error {
	if ctx == nil {
		return errors.New("enroll agent: nil context")
	}
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return errors.New("enroll agent: enrollment token is invalid")
	}
	prepared, err := prepareStateDirectory(stateDirectory)
	if err != nil {
		return fmt.Errorf("enroll agent: %w", err)
	}
	if _, statErr := os.Stat(filepath.Join(prepared, agentCredentialsFilename)); statErr == nil {
		return errors.New("enroll agent: state directory is already enrolled")
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("enroll agent: inspect existing credentials: %w", statErr)
	}
	pending, err := loadOrCreatePendingEnrollment(prepared, values)
	if err != nil {
		return fmt.Errorf("enroll agent: %w", err)
	}
	baseURL, err := parseServerURL(pending.ServerURL)
	if err != nil {
		return fmt.Errorf("enroll agent: %w", err)
	}
	client, err := newTLSHTTPClient(pending.ServerCAPEM, nil)
	if err != nil {
		return fmt.Errorf("enroll agent: %w", err)
	}
	defer client.CloseIdleConnections()
	var response sessionResponse
	if err = performJSON(ctx, client, baseURL, http.MethodPost, "/v1/agent/enroll", "Jobman-Enrollment "+token, pending.Request, &response); err != nil {
		return fmt.Errorf("enroll agent: %w", err)
	}
	if response.APIVersion != controlAPIVersion || response.Kind != "AgentSession" ||
		response.Metadata.AgentID == "" || response.Metadata.SessionID == "" ||
		response.Metadata.ExpiresAt.IsZero() || response.Spec.Token == "" ||
		response.Spec.AuthScheme != "Jobman-Agent" || response.Spec.InertOnly ||
		response.Spec.Certificate.CertificatePEM == "" || response.Spec.Certificate.ExpiresAt.IsZero() {
		return errors.New("enroll agent: control server returned an invalid session")
	}
	if err = saveCredentials(prepared, credentialFiles{
		Metadata: metadata{
			ServerURL: pending.ServerURL, AgentID: response.Metadata.AgentID,
			TargetGenerationID:   pending.TargetGenerationID,
			SessionID:            response.Metadata.SessionID,
			SessionToken:         response.Spec.Token,
			SessionExpiresAt:     response.Metadata.ExpiresAt.UTC(),
			CertificateExpiresAt: response.Spec.Certificate.ExpiresAt.UTC(),
		},
		PrivateKeyPEM: pending.PrivateKeyPEM, CertificatePEM: []byte(response.Spec.Certificate.CertificatePEM),
		ServerCAPEM: pending.ServerCAPEM,
	}); err != nil {
		return fmt.Errorf("enroll agent: %w", err)
	}
	if err = removePrivateFile(filepath.Join(prepared, pendingEnrollmentFilename)); err != nil {
		return fmt.Errorf("enroll agent: remove pending enrollment: %w", err)
	}

	return nil
}

func loadOrCreatePendingEnrollment(stateDirectory string, values Enrollment) (pendingEnrollment, error) {
	pendingPath := filepath.Join(stateDirectory, pendingEnrollmentFilename)
	document, err := os.ReadFile(pendingPath)
	if err == nil {
		var pending pendingEnrollment
		if err = decodeStrictJSON(document, &pending); err != nil {
			return pendingEnrollment{}, fmt.Errorf("decode pending enrollment: %w", err)
		}
		if pending.ServerURL != values.ServerURL || pending.TargetGenerationID != values.TargetGenerationID ||
			len(pending.PrivateKeyPEM) == 0 || pending.Request.Spec.CertificateSigningRequest == "" {
			return pendingEnrollment{}, errors.New("pending enrollment conflicts with requested server or target")
		}

		return pending, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return pendingEnrollment{}, fmt.Errorf("read pending enrollment: %w", err)
	}
	privateKey, certificateRequest, err := generateKeyAndCSR(values.Hostname)
	if err != nil {
		return pendingEnrollment{}, err
	}
	sort.Strings(values.ExecutionBackends)
	sort.Strings(values.Runtimes)
	sort.Strings(values.Capabilities)
	pending := pendingEnrollment{
		ServerURL: values.ServerURL, TargetGenerationID: values.TargetGenerationID,
		PrivateKeyPEM: privateKey, ServerCAPEM: values.ServerCAPEM,
		Request: enrollmentRequest{
			APIVersion: controlAPIVersion, Kind: "AgentEnrollment",
			Spec: enrollmentRequestSpec{
				TargetGenerationID: values.TargetGenerationID, AgentVersion: values.AgentVersion,
				ProtocolVersions: []string{protocol.V1Alpha1},
				Host: enrollmentHost{
					OperatingSystem: values.OperatingSystem, Architecture: values.Architecture,
					Hostname: values.Hostname,
				},
				ExecutionUser: values.ExecutionUser, ExecutionBackends: values.ExecutionBackends,
				Runtimes: values.Runtimes, Capabilities: values.Capabilities,
				CertificateSigningRequest: certificateRequest,
			},
		},
	}
	encoded, err := json.Marshal(pending)
	if err != nil {
		return pendingEnrollment{}, fmt.Errorf("encode pending enrollment: %w", err)
	}
	if err = writePrivateFile(pendingPath, encoded); err != nil {
		return pendingEnrollment{}, fmt.Errorf("write pending enrollment: %w", err)
	}

	return pending, nil
}

// Client is an authenticated Jobman Control agent client.
type Client struct {
	baseURL     *url.URL
	httpClient  *http.Client
	credentials credentialFiles
}

// CapabilityReport describes the current, bounded data-plane facts advertised
// by an agent. Server policy can narrow but never expand these observations.
type CapabilityReport struct {
	ObservedAt           time.Time
	AcceptingAssignments bool
	AgentVersion         string
	OperatingSystem      string
	Architecture         string
	Hostname             string
	ExecutionUser        string
	ExecutionBackends    []string
	Runtimes             []string
	Capabilities         []string
}

// OpenClient loads credentials from a host-local state directory.
func OpenClient(stateDirectory string) (*Client, error) {
	credentials, err := loadCredentials(stateDirectory)
	if err != nil {
		return nil, fmt.Errorf("open agent client: %w", err)
	}
	baseURL, err := parseServerURL(credentials.Metadata.ServerURL)
	if err != nil {
		return nil, fmt.Errorf("open agent client: %w", err)
	}
	httpClient, err := newMTLSHTTPClient(credentials)
	if err != nil {
		return nil, fmt.Errorf("open agent client: %w", err)
	}

	return &Client{baseURL: baseURL, httpClient: httpClient, credentials: credentials}, nil
}

// Close releases idle transport connections.
func (client *Client) Close() {
	if client != nil && client.httpClient != nil {
		client.httpClient.CloseIdleConnections()
	}
}

// AgentID returns the stable enrolled agent identity.
func (client *Client) AgentID() string { return client.credentials.Metadata.AgentID }

// TargetGenerationID returns the immutable enrolled target generation.
func (client *Client) TargetGenerationID() string {
	return client.credentials.Metadata.TargetGenerationID
}

// CertificateExpiresAt reports when the current mTLS credential expires.
func (client *Client) CertificateExpiresAt() time.Time {
	return client.credentials.Metadata.CertificateExpiresAt
}

// SessionExpiresAt reports when the inert, liveness-only agent session
// expires. Execution authorization continues to use mTLS.
func (client *Client) SessionExpiresAt() time.Time {
	return client.credentials.Metadata.SessionExpiresAt
}

// ReportCapabilities publishes one immutable, replay-safe capability
// observation through the authenticated agent API.
func (client *Client) ReportCapabilities(ctx context.Context, report CapabilityReport) error {
	request := struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			AgentID    string    `json:"agentId"`
			ObservedAt time.Time `json:"observedAt"`
		} `json:"metadata"`
		Spec struct {
			AcceptingAssignments bool           `json:"acceptingAssignments"`
			AgentVersion         string         `json:"agentVersion"`
			Host                 enrollmentHost `json:"host"`
			ExecutionUser        string         `json:"executionUser"`
			ExecutionBackends    []string       `json:"executionBackends"`
			Runtimes             []string       `json:"runtimes"`
			Capabilities         []string       `json:"capabilities"`
		} `json:"spec"`
	}{APIVersion: controlAPIVersion, Kind: "AgentCapabilities"}
	request.Metadata.AgentID = client.AgentID()
	request.Metadata.ObservedAt = report.ObservedAt.UTC()
	request.Spec.AcceptingAssignments = report.AcceptingAssignments
	request.Spec.AgentVersion = report.AgentVersion
	request.Spec.Host = enrollmentHost{
		OperatingSystem: report.OperatingSystem, Architecture: report.Architecture,
		Hostname: report.Hostname,
	}
	request.Spec.ExecutionUser = report.ExecutionUser
	request.Spec.ExecutionBackends = append([]string(nil), report.ExecutionBackends...)
	request.Spec.Runtimes = append([]string(nil), report.Runtimes...)
	request.Spec.Capabilities = append([]string(nil), report.Capabilities...)
	sort.Strings(request.Spec.ExecutionBackends)
	sort.Strings(request.Spec.Runtimes)
	sort.Strings(request.Spec.Capabilities)
	var receipt struct {
		APIVersion string    `json:"apiVersion"`
		Kind       string    `json:"kind"`
		AgentID    string    `json:"agentId"`
		Revision   int64     `json:"revision"`
		ObservedAt time.Time `json:"observedAt"`
	}
	if err := performJSON(
		ctx, client.httpClient, client.baseURL, http.MethodPut,
		"/v1/agent/capabilities", "", request, &receipt,
	); err != nil {
		return fmt.Errorf("report agent capabilities: %w", err)
	}
	if receipt.APIVersion != controlAPIVersion || receipt.Kind != "AgentCapabilityReceipt" ||
		receipt.AgentID != client.AgentID() || receipt.Revision < 1 ||
		!receipt.ObservedAt.Equal(request.Metadata.ObservedAt) {
		return errors.New("report agent capabilities: control server returned an invalid receipt")
	}

	return nil
}

// RenewSession rotates the inert agent liveness token and persists the new
// value before it becomes eligible for coordinator selection.
func (client *Client) RenewSession(ctx context.Context, stateDirectory string) error {
	var response sessionResponse
	if err := performJSON(
		ctx, client.httpClient, client.baseURL, http.MethodPost, "/v1/agent/session/renew",
		"Jobman-Agent "+client.credentials.Metadata.SessionToken, nil, &response,
	); err != nil {
		return fmt.Errorf("renew agent session: %w", err)
	}
	if response.APIVersion != controlAPIVersion || response.Kind != "AgentSession" ||
		response.Metadata.AgentID != client.AgentID() || response.Metadata.SessionID == "" ||
		response.Metadata.ExpiresAt.IsZero() || response.Spec.Token == "" ||
		response.Spec.AuthScheme != "Jobman-Agent" || !response.Spec.InertOnly {
		return errors.New("renew agent session: control server returned an invalid session")
	}
	updated := client.credentials
	updated.Metadata.SessionID = response.Metadata.SessionID
	updated.Metadata.SessionToken = response.Spec.Token
	updated.Metadata.SessionExpiresAt = response.Metadata.ExpiresAt.UTC()
	if err := saveCredentials(stateDirectory, updated); err != nil {
		return fmt.Errorf("renew agent session: %w", err)
	}
	client.credentials = updated

	return nil
}

// ListAssignments returns redeliverable inert offers for this agent.
func (client *Client) ListAssignments(ctx context.Context, limit int) ([]protocol.SealedAgentAssignment, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("list assignments: limit must be between 1 and 100")
	}
	var response struct {
		APIVersion         string            `json:"apiVersion"`
		Kind               string            `json:"kind"`
		RequiresAcceptance bool              `json:"requiresAcceptance"`
		Items              []json.RawMessage `json:"items"`
	}
	endpoint := "/v1/agent/assignments?limit=" + strconv.Itoa(limit)
	if err := performJSON(ctx, client.httpClient, client.baseURL, http.MethodGet, endpoint, "", nil, &response); err != nil {
		return nil, fmt.Errorf("list assignments: %w", err)
	}
	if response.APIVersion != controlAPIVersion || response.Kind != "AgentAssignmentList" || !response.RequiresAcceptance {
		return nil, errors.New("list assignments: control server returned an invalid list")
	}
	result := make([]protocol.SealedAgentAssignment, 0, len(response.Items))
	for _, document := range response.Items {
		assignment, err := protocol.DecodeAgentAssignment(bytes.NewReader(document), protocol.DecodeLimits{})
		if err != nil {
			return nil, fmt.Errorf("list assignments: decode item: %w", err)
		}
		if assignment.Document.Metadata.AgentID != client.AgentID() ||
			assignment.Document.Spec.EffectiveExecution.Spec.Placement.TargetGenerationID != client.TargetGenerationID() {
			return nil, errors.New("list assignments: assignment identity does not match enrolled agent")
		}
		result = append(result, assignment)
	}

	return result, nil
}

// AcceptAssignment performs the durable acceptance handshake.
func (client *Client) AcceptAssignment(
	ctx context.Context,
	assignment protocol.SealedAgentAssignment,
) (protocol.LaunchAuthorization, error) {
	value := assignment.Document
	request := protocol.AgentAcceptance{
		APIVersion: protocol.V1Alpha1, Kind: protocol.AgentAcceptanceKind,
		Metadata: protocol.AgentAcceptanceMetadata{
			DeliveryID:  value.Metadata.DeliveryID,
			ExecutionID: value.Spec.EffectiveExecution.Metadata.ExecutionID,
			AgentID:     value.Metadata.AgentID,
		},
		Spec: protocol.AgentAcceptanceSpec{
			TargetGenerationID:       value.Spec.EffectiveExecution.Spec.Placement.TargetGenerationID,
			EffectiveExecutionDigest: assignment.EffectiveExecutionDigest,
		},
	}
	sealed, err := protocol.SealAgentAcceptance(request)
	if err != nil {
		return protocol.LaunchAuthorization{}, fmt.Errorf("accept assignment: %w", err)
	}
	var response protocol.LaunchAuthorization
	endpoint := "/v1/agent/assignments/" + url.PathEscape(value.Metadata.DeliveryID) + "/accept"
	if err = performCanonicalJSON(ctx, client.httpClient, client.baseURL, http.MethodPost, endpoint, sealed.CanonicalJSON, &response); err != nil {
		return protocol.LaunchAuthorization{}, fmt.Errorf("accept assignment: %w", err)
	}
	if err = protocol.ValidateLaunchAuthorization(response); err != nil {
		return protocol.LaunchAuthorization{}, fmt.Errorf("accept assignment: %w", err)
	}
	if response.Metadata.ExecutionID != request.Metadata.ExecutionID ||
		response.Metadata.AgentID != request.Metadata.AgentID ||
		response.Spec.TargetGenerationID != request.Spec.TargetGenerationID ||
		response.Spec.EffectiveExecutionDigest != request.Spec.EffectiveExecutionDigest {
		return protocol.LaunchAuthorization{}, errors.New("accept assignment: launch authorization does not match assignment")
	}

	return response, nil
}

// RecordEvent delivers one replay-safe execution observation.
func (client *Client) RecordEvent(ctx context.Context, event protocol.SealedExecutionEvent) error {
	value := event.Document
	if value.Metadata.AgentID != client.AgentID() {
		return errors.New("record event: event agent identity mismatch")
	}
	endpoint := "/v1/agent/executions/" + url.PathEscape(value.Metadata.ExecutionID) + "/events"
	var receipt struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		EventID    string `json:"eventId"`
		Sequence   int64  `json:"sequence"`
	}
	if err := performCanonicalJSON(ctx, client.httpClient, client.baseURL, http.MethodPost, endpoint, event.CanonicalJSON, &receipt); err != nil {
		return fmt.Errorf("record event: %w", err)
	}
	if receipt.APIVersion != controlAPIVersion || receipt.Kind != "ExecutionEventReceipt" ||
		receipt.EventID != value.Metadata.EventID || receipt.Sequence != value.Metadata.Sequence {
		return errors.New("record event: control server returned an invalid receipt")
	}

	return nil
}

// commitLogChunk records metadata only after the immutable object is present
// in the configured filesystem artifact store.
func (client *Client) commitLogChunk(ctx context.Context, chunk agentLogChunk) error {
	if err := validateAgentLogChunk(chunk); err != nil {
		return fmt.Errorf("commit log chunk: %w", err)
	}
	encoded, err := json.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("commit log chunk: %w", err)
	}
	endpoint := "/v1/agent/executions/" + url.PathEscape(chunk.Metadata.ExecutionID) + "/logs/" +
		url.PathEscape(chunk.Metadata.Stream) + "/chunks/" + strconv.FormatInt(chunk.Metadata.Sequence, 10)
	var receipt struct {
		APIVersion  string `json:"apiVersion"`
		Kind        string `json:"kind"`
		ExecutionID string `json:"executionId"`
		Stream      string `json:"stream"`
		Sequence    int64  `json:"sequence"`
	}
	if err = performCanonicalJSON(
		ctx, client.httpClient, client.baseURL, http.MethodPut, endpoint, encoded, &receipt,
	); err != nil {
		return fmt.Errorf("commit log chunk: %w", err)
	}
	if receipt.APIVersion != controlAPIVersion || receipt.Kind != "LogChunkReceipt" ||
		receipt.ExecutionID != chunk.Metadata.ExecutionID || receipt.Stream != chunk.Metadata.Stream ||
		receipt.Sequence != chunk.Metadata.Sequence {
		return errors.New("commit log chunk: control server returned an invalid receipt")
	}

	return nil
}

// ListActions returns durable desired actions for accepted executions.
func (client *Client) ListActions(ctx context.Context, limit int) ([]protocol.DesiredAction, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("list actions: limit must be between 1 and 100")
	}
	var response struct {
		APIVersion string            `json:"apiVersion"`
		Kind       string            `json:"kind"`
		Items      []json.RawMessage `json:"items"`
	}
	endpoint := "/v1/agent/actions?limit=" + strconv.Itoa(limit)
	if err := performJSON(ctx, client.httpClient, client.baseURL, http.MethodGet, endpoint, "", nil, &response); err != nil {
		return nil, fmt.Errorf("list actions: %w", err)
	}
	if response.APIVersion != controlAPIVersion || response.Kind != "DesiredActionList" {
		return nil, errors.New("list actions: control server returned an invalid list")
	}
	result := make([]protocol.DesiredAction, 0, len(response.Items))
	for _, document := range response.Items {
		var action protocol.DesiredAction
		if err := decodeStrictJSON(document, &action); err != nil {
			return nil, fmt.Errorf("list actions: decode item: %w", err)
		}
		if err := protocol.ValidateDesiredAction(action); err != nil {
			return nil, fmt.Errorf("list actions: %w", err)
		}
		if action.Metadata.AgentID != client.AgentID() {
			return nil, errors.New("list actions: action agent identity mismatch")
		}
		result = append(result, action)
	}

	return result, nil
}

// AcknowledgeAction confirms that an action was committed to local storage.
func (client *Client) AcknowledgeAction(ctx context.Context, action protocol.DesiredAction) error {
	acknowledgement := protocol.ActionAcknowledgement{
		APIVersion: protocol.V1Alpha1, Kind: protocol.ActionAcknowledgementKind,
		Metadata: protocol.ActionAcknowledgementMetadata{
			ActionID: action.Metadata.ActionID, ExecutionID: action.Metadata.ExecutionID,
			AgentID: action.Metadata.AgentID, Revision: action.Metadata.Revision,
			ObservedAt: time.Now().UTC(),
		},
	}
	if err := protocol.ValidateActionAcknowledgement(acknowledgement); err != nil {
		return fmt.Errorf("acknowledge action: %w", err)
	}
	endpoint := "/v1/agent/actions/" + url.PathEscape(action.Metadata.ActionID) + "/acknowledge"
	if err := performJSON(ctx, client.httpClient, client.baseURL, http.MethodPost, endpoint, "", acknowledgement, nil); err != nil {
		return fmt.Errorf("acknowledge action: %w", err)
	}

	return nil
}

// RenewCertificate rotates the agent key and certificate while the current
// mTLS credential is still valid.
func (client *Client) RenewCertificate(ctx context.Context, stateDirectory string) error {
	privateKey, certificateRequest, err := generateKeyAndCSR(client.AgentID())
	if err != nil {
		return fmt.Errorf("renew agent certificate: %w", err)
	}
	request := struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Spec       struct {
			CertificateSigningRequest string `json:"certificateSigningRequest"`
		} `json:"spec"`
	}{APIVersion: controlAPIVersion, Kind: "AgentCertificateRenewal"}
	request.Spec.CertificateSigningRequest = certificateRequest
	var response certificateResponse
	if err = performJSON(ctx, client.httpClient, client.baseURL, http.MethodPost, "/v1/agent/certificate/renew", "", request, &response); err != nil {
		return fmt.Errorf("renew agent certificate: %w", err)
	}
	if response.CertificatePEM == "" || response.ExpiresAt.IsZero() {
		return errors.New("renew agent certificate: control server returned an invalid certificate")
	}
	updated := client.credentials
	updated.PrivateKeyPEM = privateKey
	updated.CertificatePEM = []byte(response.CertificatePEM)
	updated.Metadata.CertificateExpiresAt = response.ExpiresAt.UTC()
	if err = saveCredentials(stateDirectory, updated); err != nil {
		return fmt.Errorf("renew agent certificate: %w", err)
	}
	newHTTPClient, err := newMTLSHTTPClient(updated)
	if err != nil {
		return fmt.Errorf("renew agent certificate: activate: %w", err)
	}
	client.httpClient.CloseIdleConnections()
	client.httpClient = newHTTPClient
	client.credentials = updated

	return nil
}

func parseServerURL(value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, errors.New("control server URL must be an HTTPS origin")
	}
	parsed.Path = ""

	return parsed, nil
}

func performJSON(
	ctx context.Context,
	client *http.Client,
	baseURL *url.URL,
	method, endpoint, authorization string,
	requestDocument, responseDocument any,
) error {
	var body []byte
	var err error
	if requestDocument != nil {
		body, err = json.Marshal(requestDocument)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}

	return performRequest(ctx, client, baseURL, method, endpoint, authorization, body, responseDocument)
}

func performCanonicalJSON(
	ctx context.Context,
	client *http.Client,
	baseURL *url.URL,
	method, endpoint string,
	body []byte,
	responseDocument any,
) error {
	return performRequest(ctx, client, baseURL, method, endpoint, "", body, responseDocument)
}

func performRequest(
	ctx context.Context,
	client *http.Client,
	baseURL *url.URL,
	method, endpoint, authorization string,
	body []byte,
	responseDocument any,
) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	requestURL := *baseURL
	requestURL.Path = path.Clean(strings.Split(endpoint, "?")[0])
	if index := strings.IndexByte(endpoint, '?'); index >= 0 {
		requestURL.RawQuery = endpoint[index+1:]
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("construct request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if len(body) != 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		if _, drainErr := io.Copy(io.Discard, io.LimitReader(response.Body, maximumResponseSize)); drainErr != nil {
			return fmt.Errorf("discard control server error response: %w", drainErr)
		}
		return fmt.Errorf("control server returned HTTP %d", response.StatusCode)
	}
	limited := io.LimitReader(response.Body, maximumResponseSize+1)
	encoded, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if len(encoded) > maximumResponseSize {
		return errors.New("control server response exceeds size limit")
	}
	if responseDocument == nil {
		if len(bytes.TrimSpace(encoded)) != 0 {
			return errors.New("control server returned an unexpected response body")
		}

		return nil
	}
	if err = decodeStrictJSON(encoded, responseDocument); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

func newHTTPClientForCertificate(serverCAPEM, certificatePEM, keyPEM []byte) (*http.Client, error) {
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		return nil, err
	}

	return newTLSHTTPClient(serverCAPEM, &certificate)
}
