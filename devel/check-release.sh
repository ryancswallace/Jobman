#!/bin/sh

set -eu

die() {
	printf 'release check: %s\n' "$*" >&2
	exit 1
}

require_line() {
	file=$1
	line=$2
	grep -Fqx "$line" "$file" || die "$file is missing: $line"
}

check_metadata() {
	stable_tags=$(git tag --merged HEAD --list 'v*' --sort=v:refname |
		grep -E '^v([1-9][0-9]*\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)|0\.[1-9][0-9]*\.(0|[1-9][0-9]*))$' || true)
	[ -n "$stable_tags" ] || die 'no stable semantic-version release tag is available'

	for release_tag in $stable_tags; do
		version=${release_tag#v}
		grep -F "## [$version] - " CHANGELOG.md |
			grep -Eq '^## \[[0-9]+\.[0-9]+\.[0-9]+\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$' ||
			die "CHANGELOG.md is missing a dated $version heading"
	done
}

count_files() {
	directory=$1
	pattern=$2
	want=$3
	got=$(find "$directory" -maxdepth 1 -type f -name "$pattern" -size +0c | wc -l | tr -d ' ')
	[ "$got" -eq "$want" ] ||
		die "$directory contains $got nonempty files matching $pattern; expected $want"
}

verify_checksums() {
	manifest=$1
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum --check "$manifest"
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 --check "$manifest"
	else
		die 'neither sha256sum nor shasum is available'
	fi
}

check_artifacts() {
	dist=${1:-dist}
	[ -d "$dist" ] || die "artifact directory does not exist: $dist"

	count_files "$dist" 'jobman_*_linux_*.tar.gz' 3
	count_files "$dist" 'jobman_*_darwin_*.tar.gz' 2
	count_files "$dist" 'jobman_*_windows_*.zip' 3
	count_files "$dist" 'jobman_*_linux_*.apk' 3
	count_files "$dist" 'jobman_*_linux_*.deb' 3
	count_files "$dist" 'jobman_*_linux_*.rpm' 3
	count_files "$dist" '*.sbom.json' 17
	count_files "$dist" 'jobman_*_checksums.txt' 1

	set -- "$dist"/jobman_*_checksums.txt
	checksum_file=$1
	checksum_name=$(basename "$checksum_file")
	release_version=${checksum_name#jobman_}
	release_version=${release_version%_checksums.txt}
	if [ -z "$release_version" ] || [ "$release_version" = "$checksum_name" ]; then
		die "cannot determine release version from $checksum_name"
	fi
	(
		cd "$dist"
		verify_checksums "$checksum_name"
	) >/dev/null || die 'artifact checksum verification failed'

	set -- "$dist"/jobman_*_linux_amd64.tar.gz
	content_archive=$1
	[ "$(basename "$content_archive")" = "jobman_${release_version}_linux_amd64.tar.gz" ] ||
		die 'archive and checksum manifest versions do not match'
	host_os=$(go env GOOS)
	host_arch=$(go env GOARCH)
	case ${host_os}/${host_arch} in
		linux/386 | linux/amd64 | linux/arm64 | darwin/amd64 | darwin/arm64)
			native_archive=$dist/jobman_${release_version}_${host_os}_${host_arch}.tar.gz
			native_binary=jobman
			native_agent_binary=jobman-agent
			;;
		windows/386 | windows/amd64 | windows/arm64)
			native_archive=$dist/jobman_${release_version}_${host_os}_${host_arch}.zip
			native_binary=jobman.exe
			native_agent_binary=jobman-agent.exe
			;;
		*) die "unsupported release-check host: ${host_os}/${host_arch}" ;;
	esac
	[ -s "$native_archive" ] || die "native archive is missing: $native_archive"
	temporary=$(mktemp -d \
		"${TMPDIR:-/tmp}/jobman-release-check.XXXXXXXXXX") ||
		die 'could not create a private release-check directory'
	trap 'rm -rf "$temporary"' EXIT HUP INT TERM
	mkdir -p "$temporary/extract"
	tar -tzf "$content_archive" >"$temporary/contents"

	for required in \
		jobman jobman-agent LICENSE THIRD_PARTY_NOTICES.md README.md CHANGELOG.md CITATION.cff \
		CODE_OF_CONDUCT.md CONTRIBUTING.md RELEASE.md SECURITY.md SUPPORT.md \
		assets/logo.svg assets/logo-dark-transparent.svg \
		etc/jobman/jobman.yml docs/COMPATIBILITY.md docs/CONFIGURATION.md \
		docs/CONTAINERS.md docs/UPGRADING.md docs/design/SPEC.md \
		docs/manpage/jobman.1 docs/manpage/jobman-run.1 \
		docs/completions/bash/jobman docs/completions/zsh/_jobman \
		docs/completions/powershell/jobman.ps1
	do
		require_line "$temporary/contents" "$required"
	done
	if grep -Eq '(^|/)\.gitkeep$|^docs/completions/README\.md$' "$temporary/contents"; then
		die 'portable archive contains repository-only completion scaffolding'
	fi

	tar -xzf "$content_archive" -C "$temporary/extract" \
		CHANGELOG.md CITATION.cff THIRD_PARTY_NOTICES.md
	case $native_archive in
		*.tar.gz) tar -xzf "$native_archive" -C "$temporary/extract" "$native_binary" "$native_agent_binary" ;;
		*.zip) unzip -qq "$native_archive" "$native_binary" "$native_agent_binary" -d "$temporary/extract" ;;
	esac
	binary_version=$(
		"$temporary/extract/$native_binary" --version |
			awk 'NR == 1 { print $2 }'
	)
	[ -n "$binary_version" ] || die 'could not read the packaged binary version'
	[ "$binary_version" = "$release_version" ] ||
		die "packaged binary reports $binary_version; expected $release_version"
	agent_version=$(
		"$temporary/extract/$native_agent_binary" version |
			awk 'NR == 1 { print $2 }'
	)
	[ "$agent_version" = "$release_version" ] ||
		die "packaged agent reports $agent_version; expected $release_version"
	if printf '%s\n' "$binary_version" |
		grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
		grep -E "^## \[$binary_version\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$" \
			"$temporary/extract/CHANGELOG.md" >/dev/null ||
			die "packaged changelog has no dated $binary_version release section"
		require_line "$temporary/extract/CHANGELOG.md" \
			"[Unreleased]: https://github.com/ryancswallace/jobman/compare/v$binary_version...HEAD"
	fi
	require_line "$temporary/extract/CITATION.cff" 'cff-version: 1.2.0'
	require_line "$temporary/extract/CITATION.cff" 'title: Jobman'
	require_line "$temporary/extract/THIRD_PARTY_NOTICES.md" '# Third-Party Notices'
	require_line "$temporary/extract/THIRD_PARTY_NOTICES.md" \
		"## Go standard library go$(tr -d '[:space:]' <go.version)"
	require_line "$temporary/extract/THIRD_PARTY_NOTICES.md" '### PATENTS'

	rm -rf "$temporary"
	trap - EXIT HUP INT TERM
}

case ${1:-} in
	metadata)
		check_metadata
		;;
	artifacts)
		check_artifacts "${2:-dist}"
		;;
	*)
		die 'usage: devel/check-release.sh metadata | artifacts [DIST_DIR]'
		;;
esac
