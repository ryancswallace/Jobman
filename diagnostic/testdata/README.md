# Diagnostic evidence fixtures

These files are canonical, network-free compatibility inputs for Jobman's
public `diagnostic` package and independently released consumers. Valid files
are sealed with their semantic `evidence_id`; `manifest.json` records both that
ID and the SHA-256 of the exact encoded file.

Regenerate the valid fixtures after an intentional contract change with:

```sh
UPDATE_DIAGNOSTIC_FIXTURES=1 go test ./diagnostic -run TestEvidenceFixtures
```

Changing an existing schema-1 fixture requires compatibility review. Before a
release, replace the manifest's `unreleased` origin with the release containing
the fixture and copy the immutable files plus manifest into supported consumer
repositories.

`shared-control-failure-v2.json` is a separate schema-2 compatibility fixture
for shared Control evidence. Its exact bytes are checked by
`TestSharedEvidenceFixture`. It deliberately does not change the original
schema-1 manifest. See [the shared contract](../../docs/SHARED_DIAGNOSTIC_EVIDENCE.md)
for provenance and collector rules. Regenerate only that reviewed fixture with:

```sh
UPDATE_DIAGNOSTIC_FIXTURES=1 go test ./diagnostic -run TestSharedEvidenceFixture
```
