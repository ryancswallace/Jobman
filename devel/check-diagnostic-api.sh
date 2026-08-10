#!/bin/sh

# Reject source-incompatible changes to the public diagnostic package relative
# to the first stable release that shipped it.
set -eu

apidiff=${APIDIFF:-./bin/apidiff}
go_command=${GO:-go}
baseline=${DIAGNOSTIC_API_BASELINE:-v1.4.0}
module=github.com/ryancswallace/jobman

case ${baseline} in
	v[0-9]*.[0-9]*.[0-9]*) ;;
	*)
		echo "invalid diagnostic API baseline: ${baseline}" >&2
		exit 2
		;;
esac
if [ ! -x "${apidiff}" ]; then
	echo "apidiff executable is unavailable: ${apidiff}" >&2
	exit 2
fi
case ${apidiff} in
	/*) ;;
	*) apidiff=$(cd "$(dirname "${apidiff}")" && pwd -P)/$(basename "${apidiff}") ;;
esac

temporary=$(mktemp -d "${TMPDIR:-/tmp}/jobman-apidiff.XXXXXXXXXX")
trap 'rm -rf "${temporary}"' EXIT HUP INT TERM

"${go_command}" mod download "${module}@${baseline}"
module_cache=$("${go_command}" env GOMODCACHE)
baseline_directory=${module_cache}/${module}@${baseline}
if [ ! -d "${baseline_directory}" ]; then
	echo "downloaded diagnostic API baseline is unavailable: ${baseline_directory}" >&2
	exit 1
fi

(
	cd "${baseline_directory}"
	"${apidiff}" -w "${temporary}/baseline.api" "${module}/diagnostic"
)
"${apidiff}" -w "${temporary}/current.api" "${module}/diagnostic"

output=$("${apidiff}" -incompatible "${temporary}/baseline.api" "${temporary}/current.api")
if [ -n "${output}" ]; then
	printf '%s\n' "${output}" >&2
	exit 1
fi

printf 'Diagnostic API remains source-compatible with Jobman %s.\n' "${baseline}"
