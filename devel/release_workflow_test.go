package devel_test

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestReleasePublicationUsesDraftAwareNumericID(t *testing.T) {
	t.Parallel()

	contents := readRepositoryFile(t, "verify-publish-release.sh")
	for _, required := range []string{
		"release(tagName:$tag){databaseId isDraft tagName}",
		"releases/${release_id}/assets?per_page=100",
		"releases/${release_id}\"",
		"gh api --method PATCH",
		`{"draft":false,"prerelease":false,"make_latest":"true"}`,
		`{"draft":false,"prerelease":true,"make_latest":"false"}`,
	} {
		if !strings.Contains(contents, required) {
			t.Errorf("release publication helper is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"/releases/tags/",
		"gh release edit",
		`"tag_name":`,
		`"target_commitish":`,
	} {
		if strings.Contains(contents, forbidden) {
			t.Errorf("release publication helper contains unsafe publication operation %q", forbidden)
		}
	}

	info, err := os.Stat("verify-publish-release.sh")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Error("release publication helper is not executable")
	}
}

func TestReleaseWorkflowsShareProtectedPublicationHelper(t *testing.T) {
	t.Parallel()

	releaseWorkflow := readRepositoryFile(t, "../.github/workflows/release.yml")
	for _, required := range []string{
		"run: ./devel/verify-publish-release.sh",
		`PROMOTE_LATEST: "true"`,
		"ref: ${{ needs.release.outputs.source_commit }}",
	} {
		if !strings.Contains(releaseWorkflow, required) {
			t.Errorf("normal release workflow is missing %q", required)
		}
	}

	recoveryWorkflow := readRepositoryFile(
		t,
		"../.github/workflows/publish-staged-release.yml",
	)
	for _, required := range []string{
		"group: jobman-release",
		"environment:",
		"name: main",
		"actions: read",
		"contents: write",
		"api-key: ${{ secrets.CLOUDSMITH_API_KEY }}",
		"run: ./devel/verify-publish-release.sh",
		"run: ./devel/publish-cloudsmith-rpms.sh",
		`PROMOTE_LATEST: "false"`,
	} {
		if !strings.Contains(recoveryWorkflow, required) {
			t.Errorf("staged-release workflow is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"id-token: write",
		"oidc-audience:",
		"oidc-namespace:",
		"oidc-service-slug:",
		"packages: write",
		"gh release edit",
		"/releases/tags/",
	} {
		if strings.Contains(recoveryWorkflow, forbidden) {
			t.Errorf("staged-release workflow contains excessive or unsafe operation %q", forbidden)
		}
	}
}

func TestReleaseVerificationUsesCanonicalRepositoryIdentity(t *testing.T) {
	t.Parallel()

	const (
		canonicalIdentity    = "https://github.com/ryancswallace/Jobman/.github/workflows/release.yml@refs/heads/main"
		noncanonicalIdentity = "https://github.com/ryancswallace/jobman/.github/workflows/release.yml@refs/heads/main"
	)
	for _, path := range []string{
		"../.github/workflows/release.yml",
		"../.github/workflows/repair-latest.yml",
		"publish-cloudsmith-rpms.sh",
		"verify-publish-release.sh",
		"../RELEASE.md",
		"../docs/DOGFOOD.md",
	} {
		contents := readRepositoryFile(t, path)
		if !strings.Contains(contents, canonicalIdentity) {
			t.Errorf("%s is missing canonical certificate identity %q", path, canonicalIdentity)
		}
		if strings.Contains(contents, noncanonicalIdentity) {
			t.Errorf("%s contains case-mismatched certificate identity %q", path, noncanonicalIdentity)
		}
	}

	publicationHelper := readRepositoryFile(t, "verify-publish-release.sh")
	for _, required := range []string{
		`GITHUB_REPOSITORY" != "ryancswallace/Jobman"`,
		"--source-uri github.com/ryancswallace/Jobman",
		"git+https://github.com/ryancswallace/Jobman@refs/heads/main",
	} {
		if !strings.Contains(publicationHelper, required) {
			t.Errorf("release publication helper is missing canonical source identity %q", required)
		}
	}
}

func TestReleaseWorkflowRefreshesMainAtDecisionBoundaries(t *testing.T) {
	t.Parallel()

	releaseWorkflow := readRepositoryFile(t, "../.github/workflows/release.yml")
	for _, boundary := range []struct {
		decision string
		refresh  string
	}{
		{
			decision: "- name: Select release source",
			refresh:  "- name: Refresh current main for release decision",
		},
		{
			decision: "- name: Restore approved release source",
			refresh:  "- name: Refresh current main after approval",
		},
	} {
		refreshIndex := strings.Index(releaseWorkflow, boundary.refresh)
		decisionIndex := strings.Index(releaseWorkflow, boundary.decision)
		switch {
		case refreshIndex < 0:
			t.Errorf("release workflow is missing %q", boundary.refresh)
		case decisionIndex < 0:
			t.Errorf("release workflow is missing %q", boundary.decision)
		case refreshIndex > decisionIndex:
			t.Errorf("release workflow refresh %q occurs after %q", boundary.refresh, boundary.decision)
		}
	}

	const mainRefspec = "refs/heads/main:refs/remotes/origin/main"
	if count := strings.Count(releaseWorkflow, mainRefspec); count != 2 {
		t.Errorf("release workflow main refresh count = %d, want 2", count)
	}
}

func TestCloudsmithPublicationUsesProtectedAPIKeyAndIsRepairable(t *testing.T) {
	t.Parallel()

	releaseWorkflow := readRepositoryFile(t, "../.github/workflows/release.yml")
	repairWorkflow := readRepositoryFile(
		t,
		"../.github/workflows/publish-cloudsmith-rpms.yml",
	)
	recoveryWorkflow := readRepositoryFile(
		t,
		"../.github/workflows/publish-staged-release.yml",
	)
	for name, contents := range map[string]string{
		"release":  releaseWorkflow,
		"repair":   repairWorkflow,
		"recovery": recoveryWorkflow,
	} {
		for _, required := range []string{
			"environment:",
			"name: main",
			"cloudsmith-io/cloudsmith-cli-action@db783de9f6e7a445e5e31d94f4210303b48a10a3",
			"api-key: ${{ secrets.CLOUDSMITH_API_KEY }}",
			`verify-auth: "true"`,
			"publish-cloudsmith-rpms.sh",
		} {
			if !strings.Contains(contents, required) {
				t.Errorf("%s workflow is missing %q", name, required)
			}
		}
		for _, forbidden := range []string{
			"oidc-audience:",
			"oidc-namespace:",
			"oidc-service-slug:",
		} {
			if strings.Contains(contents, forbidden) {
				t.Errorf("%s workflow contains obsolete Cloudsmith OIDC input %q", name, forbidden)
			}
		}
	}
	if count := strings.Count(releaseWorkflow, "id-token: write"); count != 2 {
		t.Errorf("release workflow id-token grant count = %d, want 2", count)
	}
	for name, contents := range map[string]string{
		"repair":   repairWorkflow,
		"recovery": recoveryWorkflow,
	} {
		if strings.Contains(contents, "id-token: write") {
			t.Errorf("%s workflow retains an unnecessary id-token grant", name)
		}
	}

	helper := readRepositoryFile(t, "publish-cloudsmith-rpms.sh")
	for _, required := range []string{
		"CLOUDSMITH_API_KEY",
		"CLOUDSMITH_ORG",
		"CLOUDSMITH_SERVICE_SLUG",
		"gh release view",
		"cosign verify-blob",
		"sha256sum --check",
		"any-distro/any-version",
		"checksum_sha256",
	} {
		if !strings.Contains(helper, required) {
			t.Errorf("Cloudsmith publication helper is missing %q", required)
		}
	}
	info, err := os.Stat("publish-cloudsmith-rpms.sh")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Error("Cloudsmith publication helper is not executable")
	}
}

func TestCloudsmithPublicationAcceptsAPIKeyOrOIDCAuthentication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		environment map[string]string
		wantExit    int
		wantMessage string
	}{
		{
			name: "missing GitHub token",
			environment: map[string]string{
				"CLOUDSMITH_ORG":          "jobman",
				"CLOUDSMITH_SERVICE_SLUG": "service",
			},
			wantExit:    2,
			wantMessage: "GH_TOKEN is required",
		},
		{
			name:        "missing Cloudsmith authentication",
			environment: map[string]string{"GH_TOKEN": "test-token"},
			wantExit:    2,
			wantMessage: "Cloudsmith authentication requires",
		},
		{
			name: "API key",
			environment: map[string]string{
				"CLOUDSMITH_API_KEY": "test-key",
				"GH_TOKEN":           "test-token",
			},
			wantExit: 23,
		},
		{
			name: "OIDC",
			environment: map[string]string{
				"CLOUDSMITH_ORG":          "jobman",
				"CLOUDSMITH_SERVICE_SLUG": "service",
				"GH_TOKEN":                "test-token",
			},
			wantExit: 23,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			binDirectory := t.TempDir()
			writeExecutableFixture(t, filepath.Join(binDirectory, "gh"), "#!/bin/sh\nexit 23\n")
			environment := map[string]string{
				"CLOUDSMITH_API_KEY":      "",
				"CLOUDSMITH_ORG":          "",
				"CLOUDSMITH_SERVICE_SLUG": "",
				"GH_TOKEN":                "",
				"PATH":                    binDirectory + string(os.PathListSeparator) + os.Getenv("PATH"),
			}
			for key, value := range testCase.environment {
				environment[key] = value
			}

			command := exec.CommandContext( // #nosec G204 -- The command and arguments are repository-controlled.
				t.Context(),
				"bash",
				"./devel/publish-cloudsmith-rpms.sh",
				"v1.2.3",
			)
			command.Dir = ".."
			command.Env = testEnvironment(environment)
			output, err := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != testCase.wantExit {
				t.Fatalf("exit error = %v, want status %d\n%s", err, testCase.wantExit, output)
			}
			if testCase.wantMessage != "" && !strings.Contains(string(output), testCase.wantMessage) {
				t.Fatalf("output = %q, want %q", output, testCase.wantMessage)
			}
		})
	}
}

func TestHomebrewPublicationChecksTokenAndIsRepairable(t *testing.T) {
	t.Parallel()

	releaseWorkflow := readRepositoryFile(t, "../.github/workflows/release.yml")
	recoveryWorkflow := readRepositoryFile(
		t,
		"../.github/workflows/publish-staged-release.yml",
	)
	repairWorkflow := readRepositoryFile(
		t,
		"../.github/workflows/publish-homebrew-formula.yml",
	)
	for name, contents := range map[string]string{
		"release":  releaseWorkflow,
		"recovery": recoveryWorkflow,
		"repair":   repairWorkflow,
	} {
		for _, required := range []string{
			"secrets.HOMEBREW_TAP_TOKEN",
			"gh api repos/ryancswallace/homebrew-tap",
			"--jq '.permissions.push'",
			"repository: ryancswallace/homebrew-tap",
			"git push origin HEAD:main",
		} {
			if !strings.Contains(contents, required) {
				t.Errorf("%s workflow is missing %q", name, required)
			}
		}
	}
	for _, required := range []string{
		"name: main",
		"Validate published stable release",
		"Generate formula from published checksums",
	} {
		if !strings.Contains(repairWorkflow, required) {
			t.Errorf("Homebrew repair workflow is missing %q", required)
		}
	}
}

func TestVerifyPublishReleasePublishesExactDraft(t *testing.T) {
	t.Parallel()

	const sourceCommit = "499aa0377b1207479ad06f69968ec25eb5d71468"
	testCases := []struct {
		name          string
		tag           string
		promoteLatest bool
		prerelease    bool
	}{
		{
			name:          "stable without alias promotion",
			tag:           "v1.0.0",
			promoteLatest: false,
			prerelease:    false,
		},
		{
			name:          "stable with alias promotion",
			tag:           "v1.0.0",
			promoteLatest: true,
			prerelease:    false,
		},
		{
			name:          "prerelease",
			tag:           "v1.1.0-rc.1",
			promoteLatest: false,
			prerelease:    true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			binDirectory := filepath.Join(root, "bin")
			assetDirectory := filepath.Join(root, "assets")
			if err := os.MkdirAll(binDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(assetDirectory, 0o700); err != nil {
				t.Fatal(err)
			}

			version := strings.TrimPrefix(testCase.tag, "v")
			artifactName := "jobman_" + version + "_linux_amd64.tar.gz"
			checksumName := "jobman_" + version + "_checksums.txt"
			signatureName := checksumName + ".sigstore.json"
			artifact := []byte("verified release artifact\n")
			artifactDigest := fmt.Sprintf("%x", sha256.Sum256(artifact))
			checksum := []byte(artifactDigest + "  " + artifactName + "\n")
			fixtures := map[string][]byte{
				"1": artifact,
				"2": checksum,
				"3": []byte("sigstore bundle\n"),
				"4": []byte("{\"provenance\":\"fixture\"}\n"),
			}
			for id, contents := range fixtures {
				writeFixtureFile(t, filepath.Join(assetDirectory, id), string(contents))
			}
			assetRecords := fmt.Sprintf(
				"1\t%s\tsha256:%s\n2\t%s\t\n3\t%s\t\n4\tjobman.intoto.jsonl\t\n",
				artifactName,
				artifactDigest,
				checksumName,
				signatureName,
			)
			writeFixtureFile(
				t,
				filepath.Join(root, "asset-records.tsv"),
				assetRecords,
			)

			ghLog := filepath.Join(root, "gh.log")
			dockerLog := filepath.Join(root, "docker.log")
			writeExecutableFixture(t, filepath.Join(binDirectory, "gh"), `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$GH_LOG"
arguments=" $* "
case "$arguments" in
  *" api graphql "*)
    printf '123\ttrue\t%s\n' "$RELEASE_TAG"
    ;;
  *"/releases/123/assets?per_page=100"*)
    cat "$FIXTURE_ROOT/asset-records.tsv"
    ;;
  *"/releases/assets/"*)
    last=
    for argument in "$@"; do
      last=$argument
    done
    cat "$FIXTURE_ROOT/assets/${last##*/}"
    ;;
  *"/commits/"*)
    printf '%s\n' "$EXPECTED_SOURCE_COMMIT"
    ;;
  *" --method PATCH "*"releases/123"*)
    body=$(cat)
    printf 'PATCH_BODY=%s\n' "$body" >>"$GH_LOG"
    printf '123\tfalse\t%s\t%s\n' "$EXPECTED_PRERELEASE" "$RELEASE_TAG"
    ;;
  *"/releases/latest"*)
    printf '123\t%s\n' "$RELEASE_TAG"
    ;;
  *"/releases/123"*)
    printf '123\ttrue\t%s\n' "$RELEASE_TAG"
    ;;
  *)
    printf 'unexpected gh invocation: %s\n' "$*" >&2
    exit 1
    ;;
esac
`)
			writeExecutableFixture(t, filepath.Join(binDirectory, "docker"), `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$DOCKER_LOG"
case " $* " in
  *" buildx imagetools inspect "*)
    printf '%s\n' "$EXPECTED_IMAGE_DIGEST"
    ;;
  *" run "*" --version "*)
    printf 'jobman %s (%.12s)\n' "${RELEASE_TAG#v}" "$EXPECTED_SOURCE_COMMIT"
    ;;
  *" list --completed "*)
    printf '%s\n' '{"phase":"completed","outcome":"success"}'
    ;;
esac
`)
			writeExecutableFixture(t, filepath.Join(binDirectory, "cosign"), `#!/bin/sh
exit 0
`)
			writeExecutableFixture(t, filepath.Join(binDirectory, "slsa-verifier"), `#!/bin/sh
exit 0
`)
			writeExecutableFixture(t, filepath.Join(binDirectory, "jq"), `#!/usr/bin/env bash
set -euo pipefail
case " $* " in
  *"configSource.uri"*)
    printf '%s\n' 'git+https://github.com/ryancswallace/Jobman@refs/heads/main'
    ;;
  *"configSource.digest.sha1"*)
    printf '%s\n' "$EXPECTED_SOURCE_COMMIT"
    ;;
  *)
    exit 1
    ;;
esac
`)

			expectedImageDigest := "sha256:" + strings.Repeat("a", 64)
			command := exec.CommandContext( // #nosec G204 -- The command and arguments are repository-controlled test fixtures.
				t.Context(),
				"bash",
				"./devel/verify-publish-release.sh",
			)
			command.Dir = ".."
			command.Env = testEnvironment(map[string]string{
				"DOCKER_LOG":             dockerLog,
				"EXPECTED_IMAGE_DIGEST":  expectedImageDigest,
				"EXPECTED_PRERELEASE":    strconv.FormatBool(testCase.prerelease),
				"EXPECTED_SOURCE_COMMIT": sourceCommit,
				"FIXTURE_ROOT":           root,
				"GH_LOG":                 ghLog,
				"GH_TOKEN":               "test-token",
				"GITHUB_REPOSITORY":      "ryancswallace/Jobman",
				"PATH":                   binDirectory + string(os.PathListSeparator) + os.Getenv("PATH"),
				"PROMOTE_LATEST":         strconv.FormatBool(testCase.promoteLatest),
				"RELEASE_TAG":            testCase.tag,
				"RUNNER_TEMP":            root,
			})
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("verify-publish-release.sh: %v\n%s", err, output)
			}

			ghCalls := readFixtureFile(t, ghLog)
			expectedPayload := `{"draft":false,"prerelease":false,"make_latest":"true"}`
			if testCase.prerelease {
				expectedPayload = `{"draft":false,"prerelease":true,"make_latest":"false"}`
			}
			if !strings.Contains(ghCalls, "PATCH_BODY="+expectedPayload) {
				t.Errorf("publication request does not contain exact payload %s:\n%s", expectedPayload, ghCalls)
			}
			if strings.Contains(ghCalls, "/releases/tags/") {
				t.Errorf("publication used published-release-only tag lookup:\n%s", ghCalls)
			}

			dockerCalls := readFixtureFile(t, dockerLog)
			promoted := strings.Contains(dockerCalls, "buildx imagetools create")
			if promoted != testCase.promoteLatest {
				t.Errorf(
					"latest promotion = %t, want %t:\n%s",
					promoted,
					testCase.promoteLatest,
					dockerCalls,
				)
			}
		})
	}
}

func readRepositoryFile(t *testing.T, path string) string {
	t.Helper()

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

func readFixtureFile(t *testing.T, path string) string {
	t.Helper()

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

func writeExecutableFixture(t *testing.T, path, contents string) {
	t.Helper()

	writeFixtureFile(t, path, contents)
	if err := os.Chmod(path, 0o700); err != nil { // #nosec G302 -- Executable test shims require an execute bit.
		t.Fatal(err)
	}
}

func testEnvironment(overrides map[string]string) []string {
	environment := make(map[string]string)
	for _, record := range os.Environ() {
		key, value, found := strings.Cut(record, "=")
		if found {
			environment[key] = value
		}
	}
	for key, value := range overrides {
		environment[key] = value
	}

	records := make([]string, 0, len(environment))
	for key, value := range environment {
		records = append(records, key+"="+value)
	}
	return records
}
