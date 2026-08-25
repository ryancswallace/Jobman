package controlclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ryancswallace/jobman/internal/artifact"
	"github.com/ryancswallace/jobman/protocol"
)

const (
	maximumResponseBytes = 4 * 1024 * 1024
	maximumTokenBytes    = 64 * 1024
	maximumCABytes       = 1024 * 1024
	wireOutcomeCancelled = "cancelled" //nolint:misspell // Frozen v1alpha1 wire spelling.
)

// Options configures one client for exactly one endpoint and namespace.
type Options struct {
	Endpoint   string
	Namespace  string
	TokenFile  string
	CAFile     string
	HTTPClient *http.Client
}

// Client is safe for concurrent use and never owns shared state directly.
type Client struct {
	baseURL   *url.URL
	namespace string
	tokenFile string
	http      *http.Client
}

// APIError is a sanitized non-success response from Jobman Control.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
}

func (err *APIError) Error() string {
	if err.Code == "" {
		return fmt.Sprintf("Jobman Control returned HTTP %d", err.StatusCode)
	}

	return fmt.Sprintf("Jobman Control %s: %s", err.Code, err.Message)
}

// New validates the endpoint and constructs a bounded HTTP client.
func New(options Options) (*Client, error) {
	baseURL, err := parseEndpoint(options.Endpoint)
	if err != nil {
		return nil, err
	}
	if options.Namespace == "" || strings.ContainsAny(options.Namespace, "/\\?#") {
		return nil, errors.New("invalid Jobman Control namespace")
	}
	if baseURL.Scheme == "http" && options.TokenFile != "" {
		return nil, errors.New("jobman control token cannot be sent over HTTP")
	}
	client := options.HTTPClient
	if client == nil {
		transport, transportErr := newTransport(options.CAFile)
		if transportErr != nil {
			return nil, transportErr
		}
		client = &http.Client{Transport: transport, Timeout: 30 * time.Second}
	}
	copyOfClient := *client
	copyOfClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if copyOfClient.Timeout <= 0 {
		copyOfClient.Timeout = 30 * time.Second
	}

	return &Client{
		baseURL: baseURL, namespace: options.Namespace,
		tokenFile: options.TokenFile, http: &copyOfClient,
	}, nil
}

func parseEndpoint(endpoint string) (*url.URL, error) {
	baseURL, err := url.Parse(endpoint)
	if err != nil || baseURL.Host == "" || baseURL.User != nil || baseURL.RawQuery != "" ||
		baseURL.Fragment != "" || (baseURL.Path != "" && baseURL.Path != "/") ||
		(baseURL.Scheme != "http" && baseURL.Scheme != "https") {
		return nil, errors.New("invalid Jobman Control endpoint")
	}
	baseURL.Path = strings.TrimSuffix(baseURL.Path, "/")

	return baseURL, nil
}

func newTransport(caFile string) (*http.Transport, error) {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("default HTTP transport has an unexpected type")
	}
	copyOfTransport := transport.Clone()
	copyOfTransport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile == "" {
		return copyOfTransport, nil
	}
	certificate, _, err := readBoundedRegularFile(caFile, maximumCABytes)
	if err != nil {
		return nil, fmt.Errorf("read Jobman Control CA file: %w", err)
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("load system certificate pool: %w", err)
	}
	if !roots.AppendCertsFromPEM(certificate) {
		return nil, errors.New("jobman control CA file contains no certificates")
	}
	copyOfTransport.TLSClientConfig.RootCAs = roots

	return copyOfTransport, nil
}

// SubmitJob submits canonical portable intent with an idempotency key.
func (client *Client) SubmitJob(
	ctx context.Context,
	request protocol.SealedJobRequest,
	idempotencyKey string,
) (Job, error) {
	var job Job
	err := client.doJSON(
		ctx, http.MethodPost, client.namespacePath("jobs"),
		request.CanonicalJSON, idempotencyKey,
		[]int{http.StatusOK, http.StatusCreated}, &job, true,
	)
	if err != nil {
		return Job{}, err
	}
	if validationErr := client.validateJob(job, ""); validationErr != nil {
		return Job{}, validationErr
	}

	return job, nil
}

// SubmitCollection submits canonical portable collection intent.
func (client *Client) SubmitCollection(
	ctx context.Context,
	request protocol.SealedCollectionRequest,
	idempotencyKey string,
) (Collection, error) {
	var collection Collection
	err := client.doJSON(
		ctx, http.MethodPost, client.namespacePath("collections"),
		request.CanonicalJSON, idempotencyKey,
		[]int{http.StatusOK, http.StatusCreated}, &collection, true,
	)
	if err != nil {
		return Collection{}, err
	}
	if validationErr := client.validateCollection(collection, ""); validationErr != nil {
		return Collection{}, validationErr
	}

	return collection, nil
}

// GetCollection returns one aggregate and its ordered child snapshots.
func (client *Client) GetCollection(ctx context.Context, collectionID string) (Collection, error) {
	var collection Collection
	err := client.doJSON(
		ctx, http.MethodGet, client.namespacePath("collections", collectionID), nil, "",
		[]int{http.StatusOK}, &collection, false,
	)
	if err != nil {
		return Collection{}, err
	}
	if validationErr := client.validateCollection(collection, collectionID); validationErr != nil {
		return Collection{}, validationErr
	}

	return collection, nil
}

// SubmitGraph submits one canonical immutable dependency graph.
func (client *Client) SubmitGraph(
	ctx context.Context,
	request protocol.SealedGraphRequest,
	idempotencyKey string,
) (Graph, error) {
	var graph Graph
	err := client.doJSON(
		ctx, http.MethodPost, client.namespacePath("graphs"), request.CanonicalJSON,
		idempotencyKey, []int{http.StatusOK, http.StatusCreated}, &graph, true,
	)
	if err != nil {
		return Graph{}, err
	}
	if validationErr := client.validateGraph(graph, ""); validationErr != nil {
		return Graph{}, validationErr
	}

	return graph, nil
}

// GetGraph returns one graph aggregate and ordered node snapshots.
func (client *Client) GetGraph(ctx context.Context, graphID string) (Graph, error) {
	var graph Graph
	err := client.doJSON(
		ctx, http.MethodGet, client.namespacePath("graphs", graphID), nil, "",
		[]int{http.StatusOK}, &graph, false,
	)
	if err != nil {
		return Graph{}, err
	}
	if validationErr := client.validateGraph(graph, graphID); validationErr != nil {
		return Graph{}, validationErr
	}

	return graph, nil
}

// ListJobs returns one authorized page of shared jobs.
func (client *Client) ListJobs(
	ctx context.Context,
	limit int,
	phase string,
	pageToken string,
) (JobPage, error) {
	query := url.Values{"limit": {strconv.Itoa(limit)}}
	if phase != "" {
		query.Set("phase", phase)
	}
	if pageToken != "" {
		query.Set("pageToken", pageToken)
	}
	var page JobPage
	err := client.doJSON(
		ctx, http.MethodGet, client.namespacePath("jobs")+"?"+query.Encode(),
		nil, "", []int{http.StatusOK}, &page, false,
	)
	if err != nil {
		return JobPage{}, err
	}
	if page.APIVersion != apiVersion || page.Kind != "JobList" {
		return JobPage{}, errors.New("jobman control returned an incompatible job list")
	}
	for _, job := range page.Items {
		if validationErr := client.validateJob(job, ""); validationErr != nil {
			return JobPage{}, validationErr
		}
	}

	return page, nil
}

// GetJob returns one authorized shared job snapshot.
func (client *Client) GetJob(ctx context.Context, jobID string) (Job, error) {
	var job Job
	err := client.doJSON(
		ctx, http.MethodGet, client.namespacePath("jobs", jobID), nil, "",
		[]int{http.StatusOK}, &job, false,
	)
	if err != nil {
		return Job{}, err
	}
	if validationErr := client.validateJob(job, jobID); validationErr != nil {
		return Job{}, validationErr
	}

	return job, nil
}

// GetJobLogs returns authorized logical log metadata. Object bytes remain in
// the configured local or NFS filesystem store.
func (client *Client) GetJobLogs(ctx context.Context, jobID string) (LogManifest, error) {
	var manifest LogManifest
	err := client.doJSON(
		ctx, http.MethodGet, client.namespacePath("jobs", jobID, "logs"), nil, "",
		[]int{http.StatusOK}, &manifest, false,
	)
	if err != nil {
		return LogManifest{}, err
	}
	if err := client.validateLogManifest(manifest, jobID); err != nil {
		return LogManifest{}, err
	}

	return manifest, nil
}

// GetJobArtifacts returns authorized immutable output metadata. Object bytes
// remain in the configured local or NFS filesystem store.
func (client *Client) GetJobArtifacts(ctx context.Context, jobID string) (ArtifactManifest, error) {
	var manifest ArtifactManifest
	err := client.doJSON(
		ctx, http.MethodGet, client.namespacePath("jobs", jobID, "artifacts"), nil, "",
		[]int{http.StatusOK}, &manifest, false,
	)
	if err != nil {
		return ArtifactManifest{}, err
	}
	if err := client.validateArtifactManifest(manifest, jobID); err != nil {
		return ArtifactManifest{}, err
	}
	return manifest, nil
}

// CancelJob records durable cancellation intent with an idempotency key.
func (client *Client) CancelJob(ctx context.Context, jobID, idempotencyKey string) (Job, error) {
	var job Job
	err := client.doJSON(
		ctx, http.MethodPost, client.namespacePath("jobs", jobID, "cancel"),
		nil, idempotencyKey, []int{http.StatusOK}, &job, true,
	)
	if err != nil {
		return Job{}, err
	}
	if validationErr := client.validateJob(job, jobID); validationErr != nil {
		return Job{}, validationErr
	}

	return job, nil
}

// CancelGraph records durable cancellation intent for all nonterminal nodes.
func (client *Client) CancelGraph(ctx context.Context, graphID, idempotencyKey string) (Graph, error) {
	var graph Graph
	err := client.doJSON(
		ctx, http.MethodPost, client.namespacePath("graphs", graphID, "cancel"),
		nil, idempotencyKey, []int{http.StatusOK}, &graph, true,
	)
	if err != nil {
		return Graph{}, err
	}
	if validationErr := client.validateGraph(graph, graphID); validationErr != nil {
		return Graph{}, validationErr
	}

	return graph, nil
}

// ImportCompletedHistory validates or imports one quiescent standalone job.
// A dry run performs all service admission checks without creating state.
func (client *Client) ImportCompletedHistory(
	ctx context.Context,
	request CompletedHistoryImportRequest,
	dryRun bool,
	idempotencyKey string,
) (Job, error) {
	sealed, err := client.validateCompletedHistoryImport(request)
	if err != nil {
		return Job{}, err
	}
	request.Spec.Workload = sealed.Document.Spec.Workload
	encoded, err := json.Marshal(request)
	if err != nil {
		return Job{}, fmt.Errorf("encode completed history import: %w", err)
	}
	requestPath := client.namespacePath("history", "imports")
	if dryRun {
		return Job{}, client.dryRunCompletedHistoryImport(ctx, requestPath+"?dryRun=true", encoded)
	}
	var job Job
	err = client.doJSON(
		ctx, http.MethodPost, requestPath, encoded, idempotencyKey,
		[]int{http.StatusOK, http.StatusCreated}, &job, true,
	)
	if err != nil {
		return Job{}, err
	}
	if validationErr := client.validateJob(job, ""); validationErr != nil {
		return Job{}, validationErr
	}

	return job, nil
}

func (client *Client) validateCompletedHistoryImport(
	request CompletedHistoryImportRequest,
) (protocol.SealedJobRequest, error) {
	if request.APIVersion != apiVersion || request.Kind != "CompletedHistoryImport" ||
		request.Metadata.Namespace != client.namespace || request.Spec.CompletedAt.IsZero() ||
		request.Spec.Source.Store != "sqlite" || request.Spec.Source.Schema < 1 ||
		request.Spec.Source.JobID == "" ||
		!slices.Contains([]string{"success", "failure", wireOutcomeCancelled, "timed_out", "aborted", "lost"}, request.Spec.Outcome) {
		return protocol.SealedJobRequest{}, errors.New("completed history import is invalid")
	}
	sealed, err := protocol.SealJobRequest(protocol.JobRequest{
		APIVersion: protocol.V1Alpha1, Kind: protocol.JobRequestKind,
		Metadata: request.Metadata,
		Spec: protocol.JobRequestSpec{
			Workload: request.Spec.Workload, Placement: request.Spec.Placement,
		},
	})
	if err != nil {
		return protocol.SealedJobRequest{}, fmt.Errorf("validate completed history workload: %w", err)
	}

	return sealed, nil
}

func (client *Client) dryRunCompletedHistoryImport(ctx context.Context, requestPath string, encoded []byte) error {
	var plan struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Status     struct {
			Result string `json:"result"`
		} `json:"status"`
	}
	err := client.doJSON(
		ctx, http.MethodPost, requestPath, encoded, "", []int{http.StatusOK}, &plan, false,
	)
	if err != nil {
		return err
	}
	if plan.APIVersion != apiVersion || plan.Kind != "CompletedHistoryImportPlan" || plan.Status.Result != "valid" {
		return errors.New("jobman control returned an incompatible history import plan")
	}

	return nil
}

// ListTargets returns all namespace-visible targets.
func (client *Client) ListTargets(ctx context.Context) ([]Target, error) {
	var page targetPage
	err := client.doJSON(
		ctx, http.MethodGet, client.namespacePath("targets"), nil, "",
		[]int{http.StatusOK}, &page, false,
	)
	if err != nil {
		return nil, err
	}
	if page.APIVersion != apiVersion || page.Kind != "TargetList" {
		return nil, errors.New("jobman control returned an incompatible target list")
	}
	for _, target := range page.Items {
		if validationErr := client.validateTarget(target, ""); validationErr != nil {
			return nil, validationErr
		}
	}

	return page.Items, nil
}

// GetTarget returns one namespace-visible target.
func (client *Client) GetTarget(ctx context.Context, name string) (Target, error) {
	var target Target
	err := client.doJSON(
		ctx, http.MethodGet, client.namespacePath("targets", name), nil, "",
		[]int{http.StatusOK}, &target, false,
	)
	if err != nil {
		return Target{}, err
	}
	if validationErr := client.validateTarget(target, name); validationErr != nil {
		return Target{}, validationErr
	}

	return target, nil
}

// UpdateTargetState applies a revision-checked target lifecycle transition.
func (client *Client) UpdateTargetState(
	ctx context.Context,
	name, state string,
	revision int64,
	idempotencyKey string,
) (Target, error) {
	if name == "" || state == "" || revision < 1 {
		return Target{}, errors.New("invalid target state request")
	}
	document, err := json.Marshal(map[string]any{
		"apiVersion": apiVersion,
		"kind":       "TargetStateChange",
		"spec":       map[string]string{"state": state},
	})
	if err != nil {
		return Target{}, fmt.Errorf("encode target state request: %w", err)
	}
	var target Target
	headers := map[string]string{"If-Match": fmt.Sprintf(`"revision-%d"`, revision)}
	err = client.doJSONWithHeaders(
		ctx, http.MethodPut, client.namespacePath("targets", name, "state"),
		document, idempotencyKey, []int{http.StatusOK}, &target, true, headers,
	)
	if err != nil {
		return Target{}, err
	}
	if validationErr := client.validateTarget(target, name); validationErr != nil {
		return Target{}, validationErr
	}

	return target, nil
}

func (client *Client) namespacePath(components ...string) string {
	parts := make([]string, 0, 3+len(components))
	parts = append(parts, "v1", "namespaces", client.namespace)
	parts = append(parts, components...)
	for index := range parts {
		parts[index] = url.PathEscape(parts[index])
	}

	return "/" + path.Join(parts...)
}

func (client *Client) doJSON(
	ctx context.Context,
	method string,
	requestPath string,
	body []byte,
	idempotencyKey string,
	expected []int,
	destination any,
	retry bool,
) error {
	return client.doJSONWithHeaders(
		ctx, method, requestPath, body, idempotencyKey, expected, destination, retry, nil,
	)
}

func (client *Client) doJSONWithHeaders(
	ctx context.Context,
	method string,
	requestPath string,
	body []byte,
	idempotencyKey string,
	expected []int,
	destination any,
	retry bool,
	headers map[string]string,
) error {
	attempts := 1
	if retry {
		attempts = 2
	}
	for attempt := range attempts {
		shouldRetry, err := client.doJSONAttempt(
			ctx, method, requestPath, body, idempotencyKey, expected, destination,
			retry && attempt+1 < attempts, headers,
		)
		if shouldRetry {
			continue
		}

		return err
	}

	return errors.New("jobman control request exhausted retries")
}

func (client *Client) doJSONAttempt(
	ctx context.Context,
	method string,
	requestPath string,
	body []byte,
	idempotencyKey string,
	expected []int,
	destination any,
	canRetry bool,
	headers map[string]string,
) (bool, error) {
	request, err := client.newRequest(ctx, method, requestPath, body, idempotencyKey)
	if err != nil {
		return false, err
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := client.http.Do(request)
	if err != nil {
		if canRetry && ctx.Err() == nil {
			return true, nil
		}

		return false, fmt.Errorf("call Jobman Control: %w", err)
	}
	encoded, readErr := io.ReadAll(io.LimitReader(response.Body, maximumResponseBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		return false, fmt.Errorf("read Jobman Control response: %w", readErr)
	}
	if closeErr != nil {
		return false, fmt.Errorf("close Jobman Control response: %w", closeErr)
	}
	if len(encoded) > maximumResponseBytes {
		return false, errors.New("jobman control response exceeds size limit")
	}
	if canRetry && retryableStatus(response.StatusCode) {
		return true, nil
	}
	if !slices.Contains(expected, response.StatusCode) {
		return false, decodeAPIError(response.StatusCode, encoded)
	}
	if mediaType := response.Header.Get("Content-Type"); !strings.HasPrefix(mediaType, "application/json") {
		return false, errors.New("jobman control returned a non-JSON response")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	if err = decoder.Decode(destination); err != nil {
		return false, fmt.Errorf("decode Jobman Control response: %w", err)
	}
	if err = decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return false, errors.New("jobman control response contains trailing data")
	}

	return false, nil
}

func retryableStatus(status int) bool {
	return status == http.StatusBadGateway || status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

func (client *Client) newRequest(
	ctx context.Context,
	method string,
	requestPath string,
	body []byte,
	idempotencyKey string,
) (*http.Request, error) {
	endpoint := *client.baseURL
	pathAndQuery, err := url.Parse(requestPath)
	if err != nil {
		return nil, fmt.Errorf("construct Jobman Control URL: %w", err)
	}
	endpoint.Path = path.Join(endpoint.Path, pathAndQuery.Path)
	endpoint.RawQuery = pathAndQuery.RawQuery
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("construct Jobman Control request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	if client.tokenFile != "" {
		token, tokenErr := readTokenFile(client.tokenFile)
		if tokenErr != nil {
			return nil, tokenErr
		}
		request.Header.Set("Authorization", "Bearer "+token)
	}

	return request, nil
}

func (client *Client) validateJob(job Job, expectedID string) error {
	if job.APIVersion != apiVersion || job.Kind != "Job" || job.Metadata.ID == "" ||
		job.Metadata.Namespace != client.namespace || (expectedID != "" && job.Metadata.ID != expectedID) {
		return errors.New("jobman control returned an incompatible job")
	}
	if job.Status.ObservationConfidence != "" &&
		!slices.Contains([]string{"current", "stale", "uncertain", "lost"}, job.Status.ObservationConfidence) {
		return errors.New("jobman control returned invalid observation confidence")
	}
	if job.Status.ConfidenceUpdatedAt != nil && job.Status.ConfidenceUpdatedAt.IsZero() {
		return errors.New("jobman control returned invalid confidence timestamp")
	}

	return nil
}

//nolint:cyclop // Collection validation checks aggregate and ordered child invariants together.
func (client *Client) validateCollection(collection Collection, expectedID string) error {
	if collection.APIVersion != apiVersion || collection.Kind != "Collection" ||
		collection.Metadata.ID == "" || collection.Metadata.Namespace != client.namespace ||
		(expectedID != "" && collection.Metadata.ID != expectedID) ||
		collection.Spec.MaxActive < 1 || collection.Spec.MaxActive > collection.Status.Total ||
		!slices.Contains([]string{"continue", "fail-fast"}, collection.Spec.FailurePolicy) ||
		!slices.Contains([]string{"never", "prefer", "require"}, collection.Spec.ArrayPolicy) ||
		!slices.Contains([]string{"individual", "slurm-array"}, collection.Status.ArrayMode) ||
		collection.Status.Total != len(collection.Items) || collection.Status.Active < 0 ||
		collection.Status.Terminal < 0 || collection.Status.Terminal > collection.Status.Total {
		return errors.New("jobman control returned an incompatible collection")
	}
	for index, item := range collection.Items {
		if item.Index != index || item.Name == "" || client.validateJob(item.Job, "") != nil ||
			item.Job.Metadata.Name != item.Name {
			return errors.New("jobman control returned an invalid collection item")
		}
	}

	return nil
}

//nolint:cyclop // Graph validation checks aggregate, dependency, and ordered node invariants together.
func (client *Client) validateGraph(graph Graph, expectedID string) error {
	if graph.APIVersion != apiVersion || graph.Kind != "Graph" || graph.Metadata.ID == "" ||
		graph.Metadata.Namespace != client.namespace || (expectedID != "" && graph.Metadata.ID != expectedID) ||
		graph.Spec.MaxActive < 1 || graph.Spec.MaxActive > graph.Status.Total ||
		!slices.Contains([]string{"skip", "cancel", "blocked"}, graph.Spec.UnsatisfiedPolicy) ||
		graph.Status.Total != len(graph.Items) || graph.Status.Waiting < 0 || graph.Status.Active < 0 ||
		graph.Status.Terminal < 0 || graph.Status.Terminal > graph.Status.Total ||
		graph.Status.Waiting+graph.Status.Active+graph.Status.Terminal != graph.Status.Total {
		return errors.New("jobman control returned an incompatible graph")
	}
	names := make(map[string]struct{}, len(graph.Items))
	for index, item := range graph.Items {
		if item.Index != index || item.Name == "" || client.validateJob(item.Job, "") != nil ||
			item.Job.Metadata.Name != item.Name ||
			(item.Disposition != "" && !slices.Contains([]string{"skipped", wireOutcomeCancelled, "blocked"}, item.Disposition)) {
			return errors.New("jobman control returned an invalid graph node")
		}
		names[item.Name] = struct{}{}
	}
	for _, item := range graph.Items {
		seen := make(map[string]struct{}, len(item.Dependencies))
		for _, dependency := range item.Dependencies {
			if _, exists := names[dependency.From]; !exists || dependency.From == item.Name ||
				!slices.Contains([]string{"success", "failure", "any-terminal", "outcomes"}, dependency.Predicate) {
				return errors.New("jobman control returned an invalid graph dependency")
			}
			if _, duplicate := seen[dependency.From]; duplicate {
				return errors.New("jobman control returned a duplicate graph dependency")
			}
			seen[dependency.From] = struct{}{}
		}
	}

	return nil
}

func (client *Client) validateTarget(target Target, expectedName string) error {
	if target.APIVersion != apiVersion || target.Kind != "Target" ||
		target.Metadata.Namespace != client.namespace || target.Metadata.Name == "" ||
		(expectedName != "" && target.Metadata.Name != expectedName) {
		return errors.New("jobman control returned an incompatible target")
	}
	if !slices.Contains([]string{"", "on-prem", "aws-parallelcluster"}, target.Spec.Provider.Kind) {
		return errors.New("jobman control returned an incompatible target provider")
	}

	return nil
}

func (client *Client) validateLogManifest(manifest LogManifest, jobID string) error {
	if manifest.APIVersion != apiVersion || manifest.Kind != "JobLogManifest" ||
		manifest.Namespace != client.namespace || manifest.JobID != jobID {
		return errors.New("jobman control returned an incompatible log manifest")
	}
	seenStreams := make(map[string]struct{}, len(manifest.Items))
	for _, stream := range manifest.Items {
		key := stream.ExecutionID + "\x00" + stream.Stream
		if !validLogStream(stream) {
			return errors.New("jobman control returned invalid log stream metadata")
		}
		if _, duplicate := seenStreams[key]; duplicate {
			return errors.New("jobman control returned a duplicate log stream")
		}
		seenStreams[key] = struct{}{}
		if !validLogChunkSequence(stream.Chunks, stream.ByteLength) {
			return errors.New("jobman control returned an inconsistent log stream length")
		}
	}

	return nil
}

func validLogStream(stream LogStream) bool {
	return stream.ExecutionID != "" && stream.RunNumber > 0 &&
		(stream.Stream == "stdout" || stream.Stream == "stderr") &&
		(stream.State == "capturing" || stream.State == "complete") && stream.ByteLength >= 0 &&
		(!stream.Truncated || stream.State == "complete")
}

func validLogChunkSequence(chunks []LogChunk, expectedLength int64) bool {
	var offset int64
	for index, chunk := range chunks {
		if chunk.Sequence != int64(index+1) || chunk.StoreName == "" || chunk.StoreVersion < 1 ||
			chunk.ObjectKey == "" || chunk.ByteOffset != offset || chunk.ByteLength < 0 ||
			!artifact.ValidDigest(chunk.Checksum) || chunk.CapturedAt.IsZero() {
			return false
		}
		offset += chunk.ByteLength
	}

	return offset == expectedLength
}

//nolint:cyclop // Manifest validation keeps all untrusted metadata checks at the client boundary.
func (client *Client) validateArtifactManifest(manifest ArtifactManifest, jobID string) error {
	if manifest.APIVersion != apiVersion || manifest.Kind != "JobArtifactManifest" ||
		manifest.Namespace != client.namespace || manifest.JobID != jobID {
		return errors.New("jobman control returned an incompatible artifact manifest")
	}
	seen := make(map[string]struct{}, len(manifest.Items))
	for _, item := range manifest.Items {
		key := item.ExecutionID + "\x00" + item.Name
		if item.ExecutionID == "" || item.RunNumber < 1 || item.Name == "" ||
			item.StoreName == "" || item.StoreVersion < 1 || item.ObjectKey == "" ||
			item.ByteLength < 0 || !artifact.ValidDigest(item.Checksum) || item.PublishedAt.IsZero() {
			return errors.New("jobman control returned invalid artifact metadata")
		}
		if _, duplicate := seen[key]; duplicate {
			return errors.New("jobman control returned duplicate artifact metadata")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func decodeAPIError(status int, encoded []byte) error {
	var envelope errorEnvelope
	if err := json.Unmarshal(encoded, &envelope); err != nil || envelope.Error.Message == "" {
		return &APIError{StatusCode: status}
	}

	return &APIError{
		StatusCode: status, Code: envelope.Error.Code, Message: envelope.Error.Message,
	}
}

func readTokenFile(filePath string) (string, error) {
	pathInfo, err := os.Lstat(filePath)
	if err != nil {
		return "", fmt.Errorf("inspect Jobman Control token file: %w", err)
	}
	if !pathInfo.Mode().IsRegular() {
		return "", errors.New("jobman control token file must be a regular non-symlink file")
	}
	encoded, info, err := readBoundedRegularFile(filePath, maximumTokenBytes)
	if err != nil {
		return "", fmt.Errorf("read Jobman Control token file: %w", err)
	}
	if !os.SameFile(pathInfo, info) {
		return "", errors.New("jobman control token file changed while opening")
	}
	if runtime.GOOS != "windows" {
		if info.Mode().Perm()&0o077 != 0 {
			return "", errors.New("jobman control token file must not be accessible by group or others")
		}
	}
	token := strings.TrimSuffix(string(encoded), "\n")
	token = strings.TrimSuffix(token, "\r")
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("jobman control token file is invalid")
	}

	return token, nil
}

func readBoundedRegularFile(filePath string, maximum int64) ([]byte, fs.FileInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nil, errors.New("path is not a regular file")
	}
	encoded, readErr := io.ReadAll(io.LimitReader(file, maximum+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, nil, readErr
	}
	if closeErr != nil {
		return nil, nil, closeErr
	}
	if int64(len(encoded)) > maximum {
		return nil, nil, fmt.Errorf("file exceeds %d bytes", maximum)
	}

	return encoded, info, nil
}
