package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	executionIDLabel        = "execution ID"
	agentIDLabel            = "agent ID"
	targetGenerationIDLabel = "target generation ID"
	maximumNameBytes        = 128
	maximumDescriptionBytes = 4096
	maximumCommandArgs      = 4096
	maximumArgumentBytes    = 64 * 1024
	maximumScriptBytes      = 1024 * 1024
	maximumMapEntries       = 1024
	maximumArtifacts        = 4096
	maximumRuns             = 10_000
	maximumCollectionItems  = 10_000
	maximumGraphNodes       = 10_000
	maximumGraphEdges       = 100_000
	slurmBackend            = "slurm"
	graphPredicateOutcomes  = "outcomes"
)

var (
	namePattern        = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9._-]{0,126}[a-z0-9])?$`)
	environmentPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	quantityPattern    = regexp.MustCompile(`^[1-9]\d*(?:B|KiB|MiB|GiB|TiB|KB|MB|GB|TB)$`)
	digestPattern      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	idPattern          = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

// ValidateWorkload verifies a normalized workload's structural and semantic
// invariants. SealWorkload should normally be used for authoring input.
//
//nolint:cyclop // Contract validation reports every independent component with its own context.
func ValidateWorkload(value Workload) error {
	if value.APIVersion != V1Alpha1 {
		return fmt.Errorf("validate workload: unsupported API version %q", value.APIVersion)
	}
	if value.Kind != WorkloadKind {
		return fmt.Errorf("validate workload: kind is %q", value.Kind)
	}
	if err := validateWorkloadMetadata(value.Metadata); err != nil {
		return err
	}
	if err := validateCommand(value.Spec.Command); err != nil {
		return err
	}
	if err := validateLogicalPath(value.Spec.WorkingDirectory, "workspace"); err != nil {
		return fmt.Errorf("validate workload: working directory: %w", err)
	}
	if value.Spec.Environment != nil {
		if err := validateEnvironment(*value.Spec.Environment); err != nil {
			return err
		}
	}
	if value.Spec.Resources != nil {
		if err := validateResources(*value.Spec.Resources); err != nil {
			return err
		}
	}
	if err := validateRuntime(value.Spec.Runtime); err != nil {
		return err
	}
	if value.Spec.Artifacts != nil {
		if err := validateArtifacts(*value.Spec.Artifacts); err != nil {
			return err
		}
	}
	if err := validatePolicy(value.Spec.Policy); err != nil {
		return err
	}
	if value.Spec.Requirements != nil {
		if err := validateRequirements(*value.Spec.Requirements); err != nil {
			return err
		}
	}
	if err := validateExtensions(value.Spec.Extensions); err != nil {
		return err
	}

	return nil
}

// ValidateJobRequest verifies a normalized job request and its workload
// binding. SealJobRequest should normally be used for authoring input.
func ValidateJobRequest(value JobRequest) error {
	if value.APIVersion != V1Alpha1 {
		return fmt.Errorf("validate job request: unsupported API version %q", value.APIVersion)
	}
	if value.Kind != JobRequestKind {
		return fmt.Errorf("validate job request: kind is %q", value.Kind)
	}
	if err := validateName("namespace", value.Metadata.Namespace); err != nil {
		return fmt.Errorf("validate job request: %w", err)
	}
	if err := validateName("name", value.Metadata.Name); err != nil {
		return fmt.Errorf("validate job request: %w", err)
	}
	if err := validateStringMap("job labels", value.Metadata.Labels, maximumNameBytes); err != nil {
		return fmt.Errorf("validate job request: %w", err)
	}
	if !digestPattern.MatchString(value.Spec.Workload.Digest) {
		return errors.New("validate job request: invalid workload digest")
	}
	sealed, err := SealWorkload(value.Spec.Workload.Document)
	if err != nil {
		return fmt.Errorf("validate job request: workload: %w", err)
	}
	if value.Spec.Workload.Digest != sealed.Digest {
		return errors.New("validate job request: workload digest does not match document")
	}
	if !reflect.DeepEqual(value.Spec.Workload.Document, sealed.Document) {
		return errors.New("validate job request: workload document is not normalized")
	}
	if err := validateName("target", value.Spec.Placement.Target); err != nil {
		return fmt.Errorf("validate job request: placement: %w", err)
	}
	if value.Spec.Placement.Partition != "" {
		if err := validateName("partition", value.Spec.Placement.Partition); err != nil {
			return fmt.Errorf("validate job request: placement: %w", err)
		}
	}

	return nil
}

// ValidateCollectionRequest verifies normalized collection policy and every
// independently sealed child.
//
//nolint:cyclop // Every collection policy and independently sealed child is validated explicitly.
func ValidateCollectionRequest(value CollectionRequest) error {
	if value.APIVersion != V1Alpha1 || value.Kind != CollectionRequestKind {
		return errors.New("validate collection request: unsupported API version or kind")
	}
	if err := validateName("namespace", value.Metadata.Namespace); err != nil {
		return fmt.Errorf("validate collection request: %w", err)
	}
	if err := validateName("name", value.Metadata.Name); err != nil {
		return fmt.Errorf("validate collection request: %w", err)
	}
	if err := validateStringMap("collection labels", value.Metadata.Labels, maximumNameBytes); err != nil {
		return fmt.Errorf("validate collection request: %w", err)
	}
	if len(value.Spec.Items) == 0 || len(value.Spec.Items) > maximumCollectionItems {
		return errors.New("validate collection request: item count is out of bounds")
	}
	if value.Spec.MaxActive < 1 || value.Spec.MaxActive > len(value.Spec.Items) {
		return errors.New("validate collection request: maxActive is out of bounds")
	}
	if !slices.Contains([]string{"continue", "fail-fast"}, value.Spec.FailurePolicy) {
		return errors.New("validate collection request: failure policy is unsupported")
	}
	if !slices.Contains([]string{"never", "prefer", "require"}, value.Spec.ArrayPolicy) {
		return errors.New("validate collection request: array policy is unsupported")
	}
	seen := make(map[string]struct{}, len(value.Spec.Items))
	for _, item := range value.Spec.Items {
		if err := validateName("item", item.Name); err != nil {
			return fmt.Errorf("validate collection request: %w", err)
		}
		if _, exists := seen[item.Name]; exists {
			return fmt.Errorf("validate collection request: item %q is duplicated", item.Name)
		}
		seen[item.Name] = struct{}{}
		child := JobRequest{
			APIVersion: V1Alpha1, Kind: JobRequestKind,
			Metadata: JobRequestMetadata{Namespace: value.Metadata.Namespace, Name: item.Name},
			Spec:     JobRequestSpec{Workload: item.Workload, Placement: item.Placement},
		}
		if err := ValidateJobRequest(child); err != nil {
			return fmt.Errorf("validate collection request: item %q: %w", item.Name, err)
		}
	}

	return nil
}

// ValidateGraphRequest verifies every sealed node, edge predicate, reference,
// and the graph's acyclic invariant.
func ValidateGraphRequest(value GraphRequest) error {
	if err := validateGraphRequestHeader(value); err != nil {
		return err
	}
	nodes, err := validateGraphNodes(value)
	if err != nil {
		return err
	}
	adjacency, indegree, err := validateGraphEdges(value.Spec.Edges, nodes, len(value.Spec.Nodes))
	if err != nil {
		return err
	}
	if graphContainsCycle(adjacency, indegree) {
		return errors.New("validate graph request: dependency cycle detected")
	}

	return nil
}

func validateGraphRequestHeader(value GraphRequest) error {
	if value.APIVersion != V1Alpha1 || value.Kind != GraphRequestKind {
		return errors.New("validate graph request: unsupported API version or kind")
	}
	if err := validateName("namespace", value.Metadata.Namespace); err != nil {
		return fmt.Errorf("validate graph request: %w", err)
	}
	if err := validateName("name", value.Metadata.Name); err != nil {
		return fmt.Errorf("validate graph request: %w", err)
	}
	if err := validateStringMap("graph labels", value.Metadata.Labels, maximumNameBytes); err != nil {
		return fmt.Errorf("validate graph request: %w", err)
	}
	if len(value.Spec.Nodes) == 0 || len(value.Spec.Nodes) > maximumGraphNodes {
		return errors.New("validate graph request: node count is out of bounds")
	}
	if len(value.Spec.Edges) > maximumGraphEdges {
		return errors.New("validate graph request: edge count is out of bounds")
	}
	if value.Spec.MaxActive < 1 || value.Spec.MaxActive > len(value.Spec.Nodes) {
		return errors.New("validate graph request: maxActive is out of bounds")
	}
	if !slices.Contains([]string{"skip", "cancel", "blocked"}, value.Spec.UnsatisfiedPolicy) {
		return errors.New("validate graph request: unsatisfied policy is unsupported")
	}

	return nil
}

func validateGraphNodes(value GraphRequest) (map[string]int, error) {
	nodes := make(map[string]int, len(value.Spec.Nodes))
	for index, node := range value.Spec.Nodes {
		if err := validateName("node", node.Name); err != nil {
			return nil, fmt.Errorf("validate graph request: %w", err)
		}
		if _, exists := nodes[node.Name]; exists {
			return nil, fmt.Errorf("validate graph request: node %q is duplicated", node.Name)
		}
		nodes[node.Name] = index
		child := JobRequest{
			APIVersion: V1Alpha1, Kind: JobRequestKind,
			Metadata: JobRequestMetadata{Namespace: value.Metadata.Namespace, Name: node.Name},
			Spec:     JobRequestSpec{Workload: node.Workload, Placement: node.Placement},
		}
		if err := ValidateJobRequest(child); err != nil {
			return nil, fmt.Errorf("validate graph request: node %q: %w", node.Name, err)
		}
	}

	return nodes, nil
}

func validateGraphEdges(
	edges []GraphEdge,
	nodes map[string]int,
	nodeCount int,
) (adjacency [][]int, indegree []int, err error) {
	adjacency = make([][]int, nodeCount)
	indegree = make([]int, nodeCount)
	seen := make(map[string]struct{}, len(edges))
	for _, edge := range edges {
		from, to, validationErr := validateGraphEdge(edge, nodes, seen)
		if validationErr != nil {
			return nil, nil, validationErr
		}
		adjacency[from] = append(adjacency[from], to)
		indegree[to]++
	}

	return adjacency, indegree, nil
}

func validateGraphEdge(
	edge GraphEdge,
	nodes map[string]int,
	seen map[string]struct{},
) (from, to int, err error) {
	from, fromExists := nodes[edge.From]
	to, toExists := nodes[edge.To]
	if !fromExists || !toExists || from == to {
		return 0, 0, errors.New("validate graph request: edge contains an invalid node reference")
	}
	key := edge.From + "\x00" + edge.To
	if _, exists := seen[key]; exists {
		return 0, 0, errors.New("validate graph request: duplicate edge")
	}
	seen[key] = struct{}{}
	if !validGraphPredicate(edge.Predicate) {
		return 0, 0, errors.New("validate graph request: edge predicate is unsupported")
	}
	if err := validateGraphEdgeOutcomes(edge); err != nil {
		return 0, 0, err
	}

	return from, to, nil
}

func validateGraphEdgeOutcomes(edge GraphEdge) error {
	if edge.Predicate != graphPredicateOutcomes && len(edge.Outcomes) != 0 {
		return errors.New("validate graph request: outcomes require the outcomes predicate")
	}
	if edge.Predicate == graphPredicateOutcomes && (len(edge.Outcomes) == 0 || len(edge.Outcomes) > 6) {
		return errors.New("validate graph request: selected outcomes are invalid")
	}
	for index, outcome := range edge.Outcomes {
		if !validGraphOutcome(outcome) || index > 0 && edge.Outcomes[index-1] == outcome {
			return errors.New("validate graph request: selected outcomes are invalid")
		}
	}

	return nil
}

func validGraphPredicate(predicate string) bool {
	return slices.Contains([]string{processOutcomeSuccess, processOutcomeFailure, "any-terminal", graphPredicateOutcomes}, predicate)
}

func validGraphOutcome(outcome string) bool {
	return slices.Contains([]string{
		processOutcomeSuccess, processOutcomeFailure, processOutcomeCancelled, processOutcomeTimedOut, "aborted", processOutcomeLost,
	}, outcome)
}

func graphContainsCycle(adjacency [][]int, indegree []int) bool {
	ready := make([]int, 0, len(indegree))
	for index, degree := range indegree {
		if degree == 0 {
			ready = append(ready, index)
		}
	}
	visited := 0
	for len(ready) > 0 {
		current := ready[len(ready)-1]
		ready = ready[:len(ready)-1]
		visited++
		for _, downstream := range adjacency[current] {
			indegree[downstream]--
			if indegree[downstream] == 0 {
				ready = append(ready, downstream)
			}
		}
	}
	return visited != len(indegree)
}

// ValidateEffectiveExecution verifies a normalized server-generated
// execution specification.
//
//nolint:cyclop,gocognit // The public contract validator reports each invalid field class precisely.
func ValidateEffectiveExecution(value EffectiveExecution) error {
	if value.APIVersion != V1Alpha1 {
		return fmt.Errorf("validate effective execution: unsupported API version %q", value.APIVersion)
	}
	if value.Kind != EffectiveExecutionKind {
		return fmt.Errorf("validate effective execution: kind is %q", value.Kind)
	}
	identities := []struct {
		name  string
		value string
	}{
		{name: executionIDLabel, value: value.Metadata.ExecutionID},
		{name: "run ID", value: value.Metadata.RunID},
		{name: "job ID", value: value.Metadata.JobID},
		{name: "target ID", value: value.Spec.Placement.TargetID},
		{name: targetGenerationIDLabel, value: value.Spec.Placement.TargetGenerationID},
	}
	for _, identity := range identities {
		if !idPattern.MatchString(identity.value) {
			return fmt.Errorf("validate effective execution: invalid %s", identity.name)
		}
	}
	if err := validateName("namespace", value.Metadata.Namespace); err != nil {
		return fmt.Errorf("validate effective execution: %w", err)
	}
	if array := value.Metadata.SlurmArray; array != nil {
		if !idPattern.MatchString(array.CollectionID) || array.TaskCount < 2 || array.TaskCount > 10_000 ||
			array.TaskIndex < 0 || array.TaskIndex >= array.TaskCount ||
			array.MaxParallel < 1 || array.MaxParallel > array.TaskCount ||
			value.Spec.Placement.ExecutionBackend != slurmBackend {
			return errors.New("validate effective execution: invalid Slurm array binding")
		}
	}
	if err := validateWorkloadBinding(value.Spec.Workload); err != nil {
		return fmt.Errorf("validate effective execution: workload: %w", err)
	}
	if err := validateName("target", value.Spec.Placement.Target); err != nil {
		return fmt.Errorf("validate effective execution: placement: %w", err)
	}
	if value.Spec.Placement.Partition != "" {
		if err := validateName("partition", value.Spec.Placement.Partition); err != nil {
			return fmt.Errorf("validate effective execution: placement: %w", err)
		}
	}
	if !slices.Contains([]string{"subprocess", slurmBackend}, value.Spec.Placement.ExecutionBackend) {
		return errors.New("validate effective execution: unsupported execution backend")
	}
	seenStores := make(map[string]struct{}, len(value.Spec.ArtifactStores))
	for _, store := range value.Spec.ArtifactStores {
		if err := validateName("artifact store", store.Name); err != nil || store.Version < 1 {
			return errors.New("validate effective execution: invalid artifact store binding")
		}
		if _, duplicate := seenStores[store.Name]; duplicate {
			return errors.New("validate effective execution: duplicate artifact store binding")
		}
		seenStores[store.Name] = struct{}{}
	}
	if !slices.IsSortedFunc(value.Spec.ArtifactStores, func(left, right ArtifactStoreBinding) int {
		return strings.Compare(left.Name, right.Name)
	}) {
		return errors.New("validate effective execution: artifact stores are not normalized")
	}
	if (value.Spec.Workload.Document.Spec.Artifacts != nil) != (len(value.Spec.ArtifactStores) != 0) {
		return errors.New("validate effective execution: artifact stores do not match workload artifacts")
	}

	return nil
}

// ValidateAgentAssignment verifies a normalized inert assignment envelope.
func ValidateAgentAssignment(value AgentAssignment) error {
	if value.APIVersion != V1Alpha1 {
		return fmt.Errorf("validate agent assignment: unsupported API version %q", value.APIVersion)
	}
	if value.Kind != AgentAssignmentKind {
		return fmt.Errorf("validate agent assignment: kind is %q", value.Kind)
	}
	if !idPattern.MatchString(value.Metadata.DeliveryID) {
		return errors.New("validate agent assignment: invalid delivery ID")
	}
	if !idPattern.MatchString(value.Metadata.AgentID) {
		return errors.New("validate agent assignment: invalid agent ID")
	}
	if !digestPattern.MatchString(value.Spec.EffectiveExecutionDigest) {
		return errors.New("validate agent assignment: invalid effective execution digest")
	}
	sealed, err := SealEffectiveExecution(value.Spec.EffectiveExecution)
	if err != nil {
		return fmt.Errorf("validate agent assignment: effective execution: %w", err)
	}
	if value.Spec.EffectiveExecutionDigest != sealed.Digest {
		return errors.New("validate agent assignment: effective execution digest does not match document")
	}
	if !reflect.DeepEqual(value.Spec.EffectiveExecution, sealed.Document) {
		return errors.New("validate agent assignment: effective execution is not normalized")
	}

	return nil
}

func validateWorkloadBinding(value WorkloadBinding) error {
	if !digestPattern.MatchString(value.Digest) {
		return errors.New("invalid workload digest")
	}
	sealed, err := SealWorkload(value.Document)
	if err != nil {
		return err
	}
	if value.Digest != sealed.Digest {
		return errors.New("workload digest does not match document")
	}
	if !reflect.DeepEqual(value.Document, sealed.Document) {
		return errors.New("workload document is not normalized")
	}

	return nil
}

func validateWorkloadMetadata(value WorkloadMetadata) error {
	if value.Name != "" {
		if err := validateName("name", value.Name); err != nil {
			return fmt.Errorf("validate workload: metadata: %w", err)
		}
	}
	if len(value.Description) > maximumDescriptionBytes || strings.ContainsRune(value.Description, 0) {
		return errors.New("validate workload: metadata: invalid description")
	}
	if err := validateStringMap("labels", value.Labels, maximumNameBytes); err != nil {
		return fmt.Errorf("validate workload: metadata: %w", err)
	}
	if err := validateStringMap("annotations", value.Annotations, maximumArgumentBytes); err != nil {
		return fmt.Errorf("validate workload: metadata: %w", err)
	}

	return nil
}

func validateCommand(value Command) error {
	direct := value.Executable != ""
	shell := value.Shell != nil
	if direct == shell {
		return errors.New("validate workload: command must select exactly one of executable or shell")
	}
	if len(value.Args) > maximumCommandArgs {
		return fmt.Errorf("validate workload: command has more than %d arguments", maximumCommandArgs)
	}
	for index, argument := range value.Args {
		if len(argument) > maximumArgumentBytes || strings.ContainsRune(argument, 0) {
			return fmt.Errorf("validate workload: command argument %d is invalid", index)
		}
	}
	if direct {
		if len(value.Executable) > maximumArgumentBytes || strings.ContainsRune(value.Executable, 0) {
			return errors.New("validate workload: executable is invalid")
		}

		return nil
	}
	if len(value.Args) != 0 {
		return errors.New("validate workload: shell command cannot contain direct arguments")
	}
	if err := validateName("shell capability", value.Shell.Capability); err != nil {
		return fmt.Errorf("validate workload: command: %w", err)
	}
	if value.Shell.Script == "" || len(value.Shell.Script) > maximumScriptBytes ||
		strings.ContainsRune(value.Shell.Script, 0) {
		return errors.New("validate workload: shell script is invalid")
	}

	return nil
}

func validateEnvironment(value Environment) error {
	if value.Profile != "" {
		if err := validateName("environment profile", value.Profile); err != nil {
			return fmt.Errorf("validate workload: environment: %w", err)
		}
	}
	if err := validateEnvironmentValues(value.Values); err != nil {
		return err
	}

	return validateSecretBindings(value.Secrets)
}

func validateEnvironmentValues(values map[string]string) error {
	if len(values) > maximumMapEntries {
		return errors.New("validate workload: environment has too many values")
	}
	for name, content := range values {
		if !environmentPattern.MatchString(name) || len(name) > maximumNameBytes {
			return fmt.Errorf("validate workload: invalid environment name %q", name)
		}
		if len(content) > maximumArgumentBytes || strings.ContainsRune(content, 0) {
			return fmt.Errorf("validate workload: invalid environment value for %q", name)
		}
	}

	return nil
}

func validateSecretBindings(secrets []SecretBinding) error {
	if len(secrets) > maximumMapEntries {
		return errors.New("validate workload: environment has too many secrets")
	}
	seen := make(map[string]struct{}, len(secrets))
	for _, secret := range secrets {
		if _, duplicate := seen[secret.Name]; duplicate {
			return fmt.Errorf("validate workload: duplicate secret %q", secret.Name)
		}
		seen[secret.Name] = struct{}{}
		if err := validateSecretBinding(secret); err != nil {
			return err
		}
	}

	return nil
}

func validateSecretBinding(secret SecretBinding) error {
	if err := validateName("secret name", secret.Name); err != nil {
		return fmt.Errorf("validate workload: environment: %w", err)
	}
	if err := validateLogicalURI(secret.Source, "secret"); err != nil {
		return fmt.Errorf("validate workload: secret %q source: %w", secret.Name, err)
	}
	environmentExposure := secret.ExposeAs.Environment != ""
	fileExposure := secret.ExposeAs.File != ""
	if environmentExposure == fileExposure {
		return fmt.Errorf("validate workload: secret %q must select one exposure", secret.Name)
	}
	if environmentExposure && !environmentPattern.MatchString(secret.ExposeAs.Environment) {
		return fmt.Errorf("validate workload: secret %q has invalid environment exposure", secret.Name)
	}
	if !fileExposure {
		return nil
	}
	if err := validateLogicalPath(secret.ExposeAs.File, "workspace"); err != nil {
		return fmt.Errorf("validate workload: secret %q file exposure: %w", secret.Name, err)
	}

	return nil
}

func validateResources(value Resources) error {
	if value.CPU < 0 || value.GPU < 0 || value.Nodes < 0 || value.Tasks < 0 {
		return errors.New("validate workload: resources cannot be negative")
	}
	if value.Memory != "" && !quantityPattern.MatchString(value.Memory) {
		return errors.New("validate workload: invalid memory quantity")
	}
	if value.TemporaryStorage != "" && !quantityPattern.MatchString(value.TemporaryStorage) {
		return errors.New("validate workload: invalid temporary storage quantity")
	}
	if value.WallTime != "" {
		if err := validateDuration("wall time", value.WallTime); err != nil {
			return fmt.Errorf("validate workload: resources: %w", err)
		}
	}

	return nil
}

func validateRuntime(value Runtime) error {
	switch value.Kind {
	case "native":
		if value.Container != nil {
			return errors.New("validate workload: native runtime cannot contain container settings")
		}
	case "container":
		if value.Container == nil {
			return errors.New("validate workload: container runtime settings are required")
		}
		if value.Container.Image == "" || len(value.Container.Image) > maximumArgumentBytes ||
			strings.ContainsRune(value.Container.Image, 0) {
			return errors.New("validate workload: invalid container image")
		}
		if !slices.Contains([]string{"always", "if-not-present", "never"}, value.Container.PullPolicy) {
			return errors.New("validate workload: invalid container pull policy")
		}
		if !slices.Contains([]string{"host", "none", "restricted"}, value.Container.Network) {
			return errors.New("validate workload: invalid container network policy")
		}
	default:
		return fmt.Errorf("validate workload: unsupported runtime kind %q", value.Kind)
	}

	return nil
}

func validateArtifacts(value Artifacts) error {
	if len(value.Inputs) > maximumArtifacts || len(value.Outputs) > maximumArtifacts-len(value.Inputs) {
		return errors.New("validate workload: too many artifacts")
	}
	seen := make(map[string]struct{}, len(value.Inputs)+len(value.Outputs))
	for _, input := range value.Inputs {
		if err := claimArtifactName(seen, input.Name, "input"); err != nil {
			return err
		}
		if err := validateInputArtifact(input); err != nil {
			return err
		}
	}
	for _, output := range value.Outputs {
		if err := claimArtifactName(seen, output.Name, "output"); err != nil {
			return err
		}
		if err := validateOutputArtifact(output); err != nil {
			return err
		}
	}

	return nil
}

func claimArtifactName(seen map[string]struct{}, name, kind string) error {
	if err := validateName(kind+" artifact name", name); err != nil {
		return fmt.Errorf("validate workload: artifacts: %w", err)
	}
	if _, duplicate := seen[name]; duplicate {
		return fmt.Errorf("validate workload: duplicate artifact name %q", name)
	}
	seen[name] = struct{}{}

	return nil
}

func validateInputArtifact(input InputArtifact) error {
	if err := validateLogicalURI(input.Source, "artifact"); err != nil {
		return fmt.Errorf("validate workload: input %q source: %w", input.Name, err)
	}
	if err := validateLogicalPath(input.Target, "inputs"); err != nil {
		return fmt.Errorf("validate workload: input %q target: %w", input.Name, err)
	}
	if input.Checksum != "" && !digestPattern.MatchString(input.Checksum) {
		return fmt.Errorf("validate workload: input %q checksum is invalid", input.Name)
	}

	return nil
}

func validateOutputArtifact(output OutputArtifact) error {
	if err := validateLogicalPath(output.Source, "outputs"); err != nil {
		return fmt.Errorf("validate workload: output %q source: %w", output.Name, err)
	}
	if err := validateLogicalURI(output.Destination, "artifact"); err != nil {
		return fmt.Errorf("validate workload: output %q destination: %w", output.Name, err)
	}

	return nil
}

func validatePolicy(value ExecutionPolicy) error {
	if value.RunTimeout != "" {
		if err := validateDuration("run timeout", value.RunTimeout); err != nil {
			return fmt.Errorf("validate workload: policy: %w", err)
		}
	}
	if value.Retry.MaxRuns < 1 || value.Retry.MaxRuns > maximumRuns {
		return fmt.Errorf("validate workload: retry max runs must be between 1 and %d", maximumRuns)
	}
	if value.Retry.Backoff != "" {
		if err := validateDuration("retry backoff", value.Retry.Backoff); err != nil {
			return fmt.Errorf("validate workload: policy: %w", err)
		}
	}
	if !slices.Contains([]string{"allow-if-idempotent", "reject"}, value.DuplicateRisk) {
		return errors.New("validate workload: invalid duplicate-risk policy")
	}

	return nil
}

func validateRequirements(value Requirements) error {
	sets := []struct {
		name   string
		values []string
	}{
		{name: "operating systems", values: value.OperatingSystems},
		{name: "architectures", values: value.Architectures},
		{name: "capabilities", values: value.Capabilities},
	}
	for _, set := range sets {
		if len(set.values) > maximumMapEntries {
			return fmt.Errorf("validate workload: too many %s", set.name)
		}
		if !slices.IsSorted(set.values) || hasDuplicate(set.values) {
			return fmt.Errorf("validate workload: %s must be sorted and unique", set.name)
		}
		for _, item := range set.values {
			if err := validateName(set.name, item); err != nil {
				return fmt.Errorf("validate workload: requirements: %w", err)
			}
		}
	}

	return nil
}

func validateExtensions(value map[string]json.RawMessage) error {
	if len(value) > maximumMapEntries {
		return errors.New("validate workload: too many extensions")
	}
	for name, raw := range value {
		if err := validateName("extension", name); err != nil {
			return fmt.Errorf("validate workload: %w", err)
		}
		canonical, err := canonicalJSON(raw, defaultMaximumJSONDepth)
		if err != nil {
			return fmt.Errorf("validate workload: extension %q: %w", name, err)
		}
		if !bytes.Equal(canonical, raw) {
			return fmt.Errorf("validate workload: extension %q is not canonical", name)
		}
		var object map[string]any
		if err := json.Unmarshal(raw, &object); err != nil || object == nil {
			return fmt.Errorf("validate workload: extension %q must be an object", name)
		}
	}

	return nil
}

func validateStringMap(name string, values map[string]string, maximumValueBytes int) error {
	if len(values) > maximumMapEntries {
		return fmt.Errorf("%s has too many entries", name)
	}
	for key, value := range values {
		if err := validateName(name+" key", key); err != nil {
			return err
		}
		if len(value) > maximumValueBytes || strings.ContainsRune(value, 0) {
			return fmt.Errorf("%s value for %q is invalid", name, key)
		}
	}

	return nil
}

func validateName(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(value) > maximumNameBytes || !namePattern.MatchString(value) {
		return fmt.Errorf("%s %q is invalid", field, value)
	}

	return nil
}

func validateDuration(field, value string) error {
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return fmt.Errorf("%s must be a positive Go duration", field)
	}

	return nil
}

func validateLogicalURI(value, scheme string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != scheme || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("must be a %s URI without credentials, query, or fragment", scheme)
	}
	if err := validateName(scheme+" store", parsed.Host); err != nil {
		return err
	}
	if parsed.Path == "" || parsed.Path == "/" {
		return errors.New("URI path is required")
	}
	for _, segment := range strings.Split(strings.TrimPrefix(parsed.Path, "/"), "/") {
		if segment == "" || segment == "." || segment == ".." || strings.ContainsRune(segment, 0) {
			return errors.New("URI contains an invalid path segment")
		}
	}

	return nil
}

func validateLogicalPath(value, root string) error {
	prefix := root + ":/"
	if !strings.HasPrefix(value, prefix) {
		return fmt.Errorf("must use the %s logical root", root)
	}
	if strings.Contains(value, `\`) || strings.ContainsRune(value, 0) {
		return errors.New("contains an invalid path character")
	}
	remainder := strings.TrimPrefix(value, prefix)
	if remainder == "" {
		return nil
	}
	if path.Clean("/"+remainder) != "/"+remainder {
		return errors.New("path is not normalized")
	}
	for _, segment := range strings.Split(remainder, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return errors.New("contains an invalid path segment")
		}
	}

	return nil
}

func hasDuplicate(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index] == values[index-1] {
			return true
		}
	}

	return false
}
