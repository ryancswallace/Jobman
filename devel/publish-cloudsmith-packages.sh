#!/usr/bin/env bash

# Publish native Linux packages from one already-public stable GitHub release.
# The caller must install gh, cosign, jq, sha256sum, and the authenticated
# Cloudsmith CLI.
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
if [[ -z "${CLOUDSMITH_API_KEY:-}" ]]; then
  echo "CLOUDSMITH_API_KEY is required" >&2
  exit 2
fi

release_tag=$1
version=${release_tag#v}
repository=jobman/stable
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
git fetch --no-tags origin refs/heads/main:refs/remotes/origin/main
git fetch --force origin "refs/tags/${release_tag}:refs/tags/${release_tag}"
if ! git merge-base --is-ancestor "${release_tag}^{commit}" origin/main; then
  echo "${release_tag} must point to a commit on main" >&2
  exit 1
fi

gh release download "${release_tag}" \
  --pattern "jobman_${version}_checksums.txt" \
  --pattern "jobman_${version}_checksums.txt.sigstore.json" \
  --pattern "jobman_${version}_linux_*.apk" \
  --pattern "jobman_${version}_linux_*.deb" \
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

expected_packages=()
for architecture in 386 amd64 arm64; do
  for format in apk deb rpm; do
    expected_packages+=("jobman_${version}_linux_${architecture}.${format}")
  done
done
for filename in "${expected_packages[@]}"; do
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

for filename in "${expected_packages[@]}"; do
  gh attestation verify "${artifact_dir}/${filename}" \
    --repo ryancswallace/Jobman \
    --signer-workflow ryancswallace/Jobman/.github/workflows/release.yml \
    --source-ref refs/heads/main >/dev/null
done

for filename in "${expected_packages[@]}"; do
  package=${artifact_dir}/${filename}
  local_digest=$(sha256sum "${package}" | awk '{ print $1 }')
  digest_tag=source-sha256-${local_digest}
  remote_packages=$(cloudsmith list packages "${repository}" \
    --output-format json \
    --query "filename:^${filename}$")
  matches=$(jq --arg filename "${filename}" \
    '[.data[] | select(.filename == $filename)]' <<< "${remote_packages}")
  match_count=$(jq 'length' <<< "${matches}")

  if [[ "${match_count}" -eq 1 ]]; then
    if ! jq -e --arg digest_tag "${digest_tag}" \
      '.[0].tags.info // [] | index($digest_tag) != null' \
      <<< "${matches}" >/dev/null; then
      echo "Cloudsmith already contains ${filename} without its expected source digest" >&2
      exit 1
    fi
    echo "Cloudsmith already contains verified ${filename}."
    continue
  fi
  if [[ "${match_count}" -ne 0 ]]; then
    echo "Cloudsmith contains duplicate records for ${filename}" >&2
    exit 1
  fi

  case ${filename##*.} in
    apk)
      package_format=alpine
      upload_target=${repository}/alpine/any-version
      ;;
    deb)
      package_format=deb
      upload_target=${repository}/any-distro/any-version
      ;;
    rpm)
      package_format=rpm
      upload_target=${repository}/any-distro/any-version
      ;;
    *)
      echo "unsupported package format for ${filename}" >&2
      exit 1
      ;;
  esac
  cloudsmith push "${package_format}" "${upload_target}" "${package}" \
    --tags "stable,${digest_tag}"
done
