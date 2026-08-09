#!/usr/bin/env bash

# Verify a complete staged release and publish that exact GitHub draft.
#
# The caller must install gh, cosign, slsa-verifier, Docker Buildx, and jq.
# Versioned artifacts and images are never rebuilt here. Stable callers may
# opt into promoting the mutable GHCR latest alias after publication.
set -euo pipefail

die() {
	printf 'release publication: %s\n' "$*" >&2
	exit 1
}

require_command() {
	command -v "$1" >/dev/null 2>&1 ||
		die "required command is unavailable: $1"
}

for command_name in \
	awk cosign diff docker gh grep jq mktemp rm sha256sum slsa-verifier sort uniq
do
	require_command "$command_name"
done

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
: "${GH_TOKEN:?GH_TOKEN is required}"
: "${RELEASE_TAG:?RELEASE_TAG is required}"
: "${EXPECTED_SOURCE_COMMIT:?EXPECTED_SOURCE_COMMIT is required}"
: "${EXPECTED_IMAGE_DIGEST:?EXPECTED_IMAGE_DIGEST is required}"

promote_latest=${PROMOTE_LATEST:-false}
if [[ "$GITHUB_REPOSITORY" != "ryancswallace/Jobman" ]]; then
	die "refusing to publish an unexpected repository: $GITHUB_REPOSITORY"
fi
if [[ ! "$RELEASE_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
	die "invalid release tag: $RELEASE_TAG"
fi
if [[ ! "$EXPECTED_SOURCE_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
	die "invalid expected source commit"
fi
if [[ ! "$EXPECTED_IMAGE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]]; then
	die "invalid expected image digest"
fi
if [[ "$promote_latest" != "true" && "$promote_latest" != "false" ]]; then
	die "PROMOTE_LATEST must be true or false"
fi

repository_owner=${GITHUB_REPOSITORY%%/*}
repository_name=${GITHUB_REPOSITORY#*/}
# GraphQL variables must remain literal for gh to bind with the -F options.
# shellcheck disable=SC2016
release_record=$(gh api graphql \
	-f query='query($owner:String!,$name:String!,$tag:String!){repository(owner:$owner,name:$name){release(tagName:$tag){databaseId isDraft tagName}}}' \
	-F owner="$repository_owner" \
	-F name="$repository_name" \
	-F tag="$RELEASE_TAG" \
	--jq 'if .data.repository.release == null then "" else [.data.repository.release.databaseId, .data.repository.release.isDraft, .data.repository.release.tagName] | @tsv end')
IFS=$'\t' read -r release_id release_is_draft release_tag <<<"$release_record"
if [[ ! "$release_id" =~ ^[0-9]+$ ||
	"$release_is_draft" != "true" ||
	"$release_tag" != "$RELEASE_TAG" ]]; then
	die "could not resolve the exact draft release $RELEASE_TAG"
fi

asset_records=$(gh api --paginate \
	"repos/${GITHUB_REPOSITORY}/releases/${release_id}/assets?per_page=100" \
	--jq '.[] | [.id, .name, (.digest // "")] | @tsv')
[[ -n "$asset_records" ]] ||
	die "draft $RELEASE_TAG contains no assets"

work_root=${RUNNER_TEMP:-${TMPDIR:-/tmp}}
download_dir=$(mktemp -d \
	"${work_root%/}/jobman-release-assets.XXXXXXXXXX")
cleanup() {
	rm -rf "$download_dir"
}
trap cleanup EXIT HUP INT TERM

asset_list=
while IFS=$'\t' read -r asset_id asset_name _asset_digest; do
	if [[ ! "$asset_id" =~ ^[0-9]+$ ||
		! "$asset_name" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
		die "draft $RELEASE_TAG contains an invalid asset record"
	fi
	if [[ -e "$download_dir/$asset_name" ]]; then
		die "draft $RELEASE_TAG contains duplicate asset name $asset_name"
	fi
	gh api --method GET \
		-H "Accept: application/octet-stream" \
		"repos/${GITHUB_REPOSITORY}/releases/assets/${asset_id}" \
		>"$download_dir/$asset_name"
	[[ -s "$download_dir/$asset_name" ]] ||
		die "downloaded asset is empty: $asset_name"
	asset_list+="${asset_name}"$'\n'
done <<<"$asset_records"
asset_list=${asset_list%$'\n'}

version=${RELEASE_TAG#v}
checksum_name="jobman_${version}_checksums.txt"
signature_name="${checksum_name}.sigstore.json"
for required in \
	"$checksum_name" \
	"$signature_name" \
	jobman.intoto.jsonl
do
	if ! grep -Fqx "$required" <<<"$asset_list"; then
		die "draft $RELEASE_TAG is missing $required"
	fi
done

expected_assets="$download_dir/expected-assets.txt"
actual_assets="$download_dir/actual-assets.txt"
awk '{ print $2 }' "$download_dir/$checksum_name" >"$expected_assets"
printf '%s\n' \
	"$checksum_name" \
	"$signature_name" \
	jobman.intoto.jsonl >>"$expected_assets"
LC_ALL=C sort -u -o "$expected_assets" "$expected_assets"
printf '%s\n' "$asset_list" | LC_ALL=C sort >"$actual_assets"
if [[ -n "$(uniq -d "$actual_assets")" ]]; then
	die "draft $RELEASE_TAG contains duplicate asset names"
fi
if ! diff -u "$expected_assets" "$actual_assets"; then
	die "draft $RELEASE_TAG contains an incomplete or unchecksummed asset set"
fi

artifact_paths=()
while read -r digest artifact; do
	if [[ ! "$digest" =~ ^[0-9a-f]{64}$ ||
		! "$artifact" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
		die "invalid checksum-manifest record: $digest $artifact"
	fi
	remote_record=$(awk -F '\t' -v artifact="$artifact" '
		$2 == artifact {
			asset_id = $1
			value = $3
			matches++
		}
		END {
			if (matches != 1) exit 1
			print asset_id "\t" value
		}
	' <<<"$asset_records") ||
		die "draft $RELEASE_TAG is missing checksummed asset $artifact"
	IFS=$'\t' read -r asset_id remote_digest <<<"$remote_record"
	if [[ ! "$asset_id" =~ ^[0-9]+$ ||
		"$remote_digest" != "sha256:$digest" ]]; then
		die "remote digest for $artifact is $remote_digest; expected sha256:$digest"
	fi
	[[ -s "$download_dir/$artifact" ]] ||
		die "downloaded checksummed asset is empty: $artifact"
	artifact_paths+=("$download_dir/$artifact")
done <"$download_dir/$checksum_name"

if [[ ${#artifact_paths[@]} -eq 0 ]]; then
	die "the release checksum manifest contains no artifacts"
fi

cosign verify-blob \
	--bundle "$download_dir/$signature_name" \
	--certificate-identity \
	'https://github.com/ryancswallace/Jobman/.github/workflows/release.yml@refs/heads/main' \
	--certificate-oidc-issuer https://token.actions.githubusercontent.com \
	"$download_dir/$checksum_name"
(
	cd "$download_dir"
	sha256sum --check "$checksum_name"
)
slsa-verifier verify-artifact \
	"${artifact_paths[@]}" \
	--provenance-path "$download_dir/jobman.intoto.jsonl" \
	--source-uri github.com/ryancswallace/Jobman \
	--source-branch main

provenance_source=$(jq -er \
	'.dsseEnvelope.payload | @base64d | fromjson |
	 .predicate.invocation.configSource.uri' \
	"$download_dir/jobman.intoto.jsonl")
provenance_commit=$(jq -er \
	'.dsseEnvelope.payload | @base64d | fromjson |
	 .predicate.invocation.configSource.digest.sha1' \
	"$download_dir/jobman.intoto.jsonl")
expected_source='git+https://github.com/ryancswallace/Jobman@refs/heads/main'
if [[ "$provenance_source" != "$expected_source" ||
	"$provenance_commit" != "$EXPECTED_SOURCE_COMMIT" ]]; then
	die "SLSA provenance does not identify the approved source commit"
fi

image=ghcr.io/ryancswallace/jobman
for image_tag in "$RELEASE_TAG" "${RELEASE_TAG#v}"; do
	current_image_digest=$(docker buildx imagetools inspect \
		"${image}:${image_tag}" --format '{{.Manifest.Digest}}')
	if [[ "$current_image_digest" != "$EXPECTED_IMAGE_DIGEST" ]]; then
		die "release image $image_tag changed after verification: $current_image_digest"
	fi
done
cosign verify \
	--certificate-identity \
	'https://github.com/ryancswallace/Jobman/.github/workflows/release.yml@refs/heads/main' \
	--certificate-oidc-issuer https://token.actions.githubusercontent.com \
	"${image}@${EXPECTED_IMAGE_DIGEST}"
./devel/container-smoke.sh \
	"${image}@${EXPECTED_IMAGE_DIGEST}" \
	"${RELEASE_TAG#v}" \
	"$EXPECTED_SOURCE_COMMIT"

current_source_commit=$(gh api \
	"repos/${GITHUB_REPOSITORY}/commits/${RELEASE_TAG}" --jq .sha)
if [[ "$current_source_commit" != "$EXPECTED_SOURCE_COMMIT" ]]; then
	die "release tag $RELEASE_TAG changed after artifact verification"
fi
current_release_record=$(gh api \
	"repos/${GITHUB_REPOSITORY}/releases/${release_id}" \
	--jq '[.id, .draft, .tag_name] | @tsv')
if [[ "$current_release_record" != \
	"${release_id}"$'\ttrue\t'"${RELEASE_TAG}" ]]; then
	die "draft $RELEASE_TAG changed before publication"
fi
current_asset_records=$(gh api --paginate \
	"repos/${GITHUB_REPOSITORY}/releases/${release_id}/assets?per_page=100" \
	--jq '.[] | [.id, .name, (.digest // "")] | @tsv' | LC_ALL=C sort)
verified_asset_records=$(printf '%s\n' "$asset_records" | LC_ALL=C sort)
if [[ "$current_asset_records" != "$verified_asset_records" ]]; then
	die "draft $RELEASE_TAG assets changed during verification"
fi

if [[ "$RELEASE_TAG" == *-* ]]; then
	publish_payload='{"draft":false,"prerelease":true,"make_latest":"false"}'
	expected_prerelease=true
else
	publish_payload='{"draft":false,"prerelease":false,"make_latest":"true"}'
	expected_prerelease=false
fi
published_record=$(gh api --method PATCH \
	"repos/${GITHUB_REPOSITORY}/releases/${release_id}" \
	--input - \
	--jq '[.id, .draft, .prerelease, .tag_name] | @tsv' \
	<<<"$publish_payload")
expected_published_record="${release_id}"$'\tfalse\t'"${expected_prerelease}"$'\t'"${RELEASE_TAG}"
if [[ "$published_record" != "$expected_published_record" ]]; then
	die "GitHub did not publish the verified release as requested"
fi

if [[ "$RELEASE_TAG" != *-* ]]; then
	latest_release_record=$(gh api \
		"repos/${GITHUB_REPOSITORY}/releases/latest" \
		--jq '[.id, .tag_name] | @tsv')
	if [[ "$latest_release_record" != \
		"${release_id}"$'\t'"${RELEASE_TAG}" ]]; then
		die "GitHub did not select $RELEASE_TAG as the latest release"
	fi
	if [[ "$promote_latest" == "true" ]]; then
		docker buildx imagetools create \
			--tag "${image}:latest" \
			"${image}@${EXPECTED_IMAGE_DIGEST}"
		latest_digest=$(docker buildx imagetools inspect \
			"${image}:latest" --format '{{.Manifest.Digest}}')
		if [[ "$latest_digest" != "$EXPECTED_IMAGE_DIGEST" ]]; then
			die "latest does not resolve to the verified release digest"
		fi
	fi
fi

printf 'Published verified release %s (GitHub release %s).\n' \
	"$RELEASE_TAG" "$release_id"
