# Documentation

The published user and operator manual is available on the
[Jobman documentation site]. `site/` contains authored guides and
`devel/sitedocs/` stages selected canonical documents from this directory for
publication without duplicating their maintained source.

- `design/` records the target product model and architectural constraints.
- `DIAGNOSTIC_EVIDENCE.md` documents the implemented evidence schema, limits,
  privacy boundary, and compatibility fixtures.
- `manpage/` contains release-generated manual pages.
- `completions/` contains release-generated shell completions.

Run `make docs` to regenerate and validate documentation. Generated man pages
and completions are intentionally ignored by Git and are built from the tagged
source during a release.

[Jobman documentation site]: https://jobman.tech/
