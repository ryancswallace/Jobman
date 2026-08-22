#!/bin/sh

# Synchronize the version in go.version with every pinned Go declaration.

set -eu

EXIT_VAR_UNDEF=166
EXIT_VAR_INVALID=167

if [ -z "${GO_VERS:-}" ]
then
    echo "error: GO_VERS is undefined" >&2
    exit "$EXIT_VAR_UNDEF"
fi

VALID_VERS_REGEX='^[0-9]+\.[0-9]+(\.[0-9]+)?((rc|beta)[0-9]+)?$'
if ! printf '%s\n' "$GO_VERS" | grep -Eq "$VALID_VERS_REGEX"
then
    echo "error: GO_VERS is not a supported Go version: $GO_VERS" >&2
    exit "$EXIT_VAR_INVALID"
fi

for command in cat grep mktemp rm sed
do
    if ! command -v "$command" >/dev/null 2>&1
    then
        echo "error: required command not found: $command" >&2
        exit 127
    fi
done

temporary=
trap 'rm -f "${temporary:-}"' EXIT HUP INT TERM

replace() {
    expression=$1
    shift
    for file
    do
        temporary=$(mktemp "${file}.tmp.XXXXXXXXXX") || {
            echo "error: could not create a temporary file for $file" >&2
            exit 1
        }
        sed "$expression" "$file" > "$temporary"
        cat "$temporary" > "$file"
        rm -f "$temporary"
        temporary=
    done
}

replace_extended() {
    expression=$1
    shift
    for file
    do
        temporary=$(mktemp "${file}.tmp.XXXXXXXXXX") || {
            echo "error: could not create a temporary file for $file" >&2
            exit 1
        }
        sed -E "$expression" "$file" > "$temporary"
        cat "$temporary" > "$file"
        rm -f "$temporary"
        temporary=
    done
}

GO_LANG_VERSION=$(printf '%s\n' "$GO_VERS" | sed -E 's/^([0-9]+\.[0-9]+).*/\1/')

printf '%s\n' "$GO_VERS" > go.version
replace "s/^go .*/go $GO_LANG_VERSION/" go.mod
replace "s/^  go: \".*\"/  go: \"$GO_LANG_VERSION\"/" .golangci.yml
replace "s/^ARG GO_VERSION=.*/ARG GO_VERSION=$GO_VERS/" Dockerfile .devcontainer/Dockerfile
replace "s/^ARG GO_FEATURE_VERSION=.*/ARG GO_FEATURE_VERSION=$GO_LANG_VERSION/" \
    .devcontainer/Dockerfile
replace "s/\"GO_VERSION\": \"[^\"]*\"/\"GO_VERSION\": \"$GO_VERS\"/" \
    .devcontainer/devcontainer.json
replace "s/go-version: \"[^\"]*\"/go-version: \"$GO_VERS\"/" \
    .github/workflows/*.yml
replace_extended "s/Go [0-9]+(\.[0-9]+){1,2}/Go $GO_VERS/g" \
    .devcontainer/README.md
replace_extended "s/Go](https:\/\/go.dev\/doc\/install) [0-9]+(\.[0-9]+){1,2}/Go](https:\/\/go.dev\/doc\/install) $GO_VERS/" \
    README.md
replace_extended "s/Install Go [0-9]+(\.[0-9]+){1,2}/Install Go $GO_VERS/" \
    RELEASE.md
replace_extended "s/(requires|require) Go [0-9]+(\.[0-9]+){1,2}/\\1 Go $GO_VERS/" \
    site/index.md site/getting-started/installation.md
replace_extended "s/If Go [0-9]+(\.[0-9]+){1,2} is already installed/If Go $GO_VERS is already installed/" \
    site/getting-started/installation.md

trap - EXIT HUP INT TERM
