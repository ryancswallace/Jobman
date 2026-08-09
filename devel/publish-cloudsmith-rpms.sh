#!/usr/bin/env bash

# Publish the RPMs from one already-public stable GitHub release to Cloudsmith.
# The caller must install gh, cosign, jq, and the authenticated Cloudsmith CLI.
set -euo pipefail

if [[ $# -ne 1 ||
      ! "$1" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "usage: $0 vMAJOR.MINOR.PATCH" >&2
  exit 2
fi
if [[ -z "${GH_TOKEN:-}" ]]; then
  echo "GH_TOKEN is required" >&2
  exit 2
fi
if [[ -z "${CLOUDSMITH_API_KEY:-}" &&
      ( -z "${CLOUDSMITH_ORG:-}" || -z "${CLOUDSMITH_SERVICE_SLUG:-}" ) ]]; then
  echo "Cloudsmith authentication requires CLOUDSMITH_API_KEY or both CLOUDSMITH_ORG and CLOUDSMITH_SERVICE_SLUG" >&2
  exit 2
fi

release_tag=$1
version=${release_tag#v}
repository=jobman/stable
upload_target=${repository}/any-distro/any-version
artifact_dir=$(mktemp -d)
trap 'rm -rf "${artifact_dir}"' EXIT

release_state=$(gh release view "${release_tag}" \
  --json isDraft,isPrerelease,tagName \
  --jq '[.isDraft, .isPrerelease, .tagName] | @tsv')
IFS=$'\t' read -r is_draft is_prerelease resolved_tag <<< "${release_state}"
if [[ "${is_draft}" != "false" ||
      "${is_prerelease}" != "false" ||
      "${resolved_tag}" != "${release_tag}" ]]; then
  echo "${release_tag} is not the exact published stable release" >&2
  exit 1
fi

gh release download "${release_tag}" \
  --pattern "jobman_${version}_checksums.txt" \
  --pattern "jobman_${version}_checksums.txt.sigstore.json" \
  --pattern "jobman_${version}_linux_*.rpm" \
  --dir "${artifact_dir}"

manifest=${artifact_dir}/jobman_${version}_checksums.txt
bundle=${manifest}.sigstore.json
cosign verify-blob \
  --bundle "${bundle}" \
  --certificate-identity \
    'https://github.com/ryancswallace/Jobman/.github/workflows/release.yml@refs/heads/main' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "${manifest}"

expected_rpms=(
  "jobman_${version}_linux_386.rpm"
  "jobman_${version}_linux_amd64.rpm"
  "jobman_${version}_linux_arm64.rpm"
)
for filename in "${expected_rpms[@]}"; do
  if [[ ! -s "${artifact_dir}/${filename}" ]]; then
    echo "release is missing ${filename}" >&2
    exit 1
  fi
  if [[ $(awk -v name="${filename}" '$2 == name { count++ } END { print count + 0 }' \
    "${manifest}") -ne 1 ]]; then
    echo "checksum manifest must contain ${filename} exactly once" >&2
    exit 1
  fi
done

(
  cd "${artifact_dir}"
  sha256sum --check --ignore-missing "$(basename "${manifest}")"
)

remote_packages=$(cloudsmith list packages "${repository}" \
  --output-format json \
  --query "format:rpm AND version:^${version}$")
for filename in "${expected_rpms[@]}"; do
  rpm=${artifact_dir}/${filename}
  local_digest=$(sha256sum "${rpm}" | awk '{ print $1 }')
  matches=$(jq --arg filename "${filename}" \
    '[.data[] | select(.filename == $filename)]' <<< "${remote_packages}")
  match_count=$(jq 'length' <<< "${matches}")

  if [[ "${match_count}" -eq 1 ]]; then
    remote_digest=$(jq -r '.[0].checksum_sha256' <<< "${matches}")
    if [[ "${remote_digest}" != "${local_digest}" ]]; then
      echo "Cloudsmith already contains different bytes for ${filename}" >&2
      exit 1
    fi
    echo "Cloudsmith already contains verified ${filename}."
    continue
  fi
  if [[ "${match_count}" -ne 0 ]]; then
    echo "Cloudsmith contains duplicate records for ${filename}" >&2
    exit 1
  fi

  cloudsmith push rpm "${upload_target}" "${rpm}" --tags stable
done
