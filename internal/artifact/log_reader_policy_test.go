package artifact

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

const (
	readerTestKey    = "namespaces/research/jobs/01990000-0000-4000-8000-000000000001/executions/01990000-0000-4000-8000-000000000002/logs/stdout/00000001.chunk"
	readerTestPolicy = `{"schema_version":1,"store_name":"logs","store_version":1,"reader_uid":21901}`
)

func TestLogReaderPolicyRequiresExplicitBoundIdentity(t *testing.T) {
	if policy, err := decodeLogReaderPolicy([]byte(readerTestPolicy), "logs", 1); err != nil || policy.ReaderUID != 21901 {
		t.Fatalf("valid policy: %v, %v", policy, err)
	}
	for _, data := range []string{
		`null`, `[]`, `{}`, readerTestPolicy + ` {}`, strings.Repeat(" ", maximumLogReaderPolicyBytes+1),
		strings.Replace(readerTestPolicy, `"reader_uid":21901`, `"reader_uid":21901,"reader_uid":21901`, 1),
		strings.Replace(readerTestPolicy, `"reader_uid":21901`, `"reader_uid":0`, 1),
		strings.Replace(readerTestPolicy, `"reader_uid":21901`, `"reader_uid":4294967295`, 1),
		strings.Replace(readerTestPolicy, `"reader_uid":21901`, `"reader_uid":"21901"`, 1),
		strings.Replace(readerTestPolicy, `"reader_uid":21901`, `"reader_uid":null`, 1),
		strings.Replace(readerTestPolicy, `"reader_uid":21901`, `"reader_uid":21901,"allow_all":true`, 1),
		strings.Replace(readerTestPolicy, `"schema_version":1`, `"schema_version":2`, 1),
		strings.Replace(readerTestPolicy, `"store_name":"logs"`, `"store_name":"foreign"`, 1),
		strings.Replace(readerTestPolicy, `"store_version":1`, `"store_version":2`, 1),
		strings.Replace(readerTestPolicy, `"schema_version"`, `"SCHEMA_VERSION"`, 1),
		strings.Replace(readerTestPolicy, `"reader_uid"`, `"READER_UID"`, 1),
	} {
		if _, err := decodeLogReaderPolicy([]byte(data), "logs", 1); err == nil {
			t.Errorf("accepted invalid policy %q", data)
		}
	}
}

func TestLogReaderGrantAppliesOnlyToCanonicalLogChunks(t *testing.T) {
	if !isSharedLogKey(readerTestKey) {
		t.Fatal("rejected canonical log key")
	}
	for _, key := range []string{
		"logs/stdout/00000001.chunk", strings.Replace(readerTestKey, "/jobs/", "/artifacts/", 1),
		strings.Replace(readerTestKey, "/stdout/", "/combined/", 1), strings.Replace(readerTestKey, "00000001.chunk", "1.chunk", 1),
		strings.Replace(readerTestKey, "00000001.chunk", "00000000.chunk", 1), strings.Replace(readerTestKey, "00000001.chunk", "000000001.chunk", 1),
		strings.Replace(readerTestKey, "01990000-0000-4000-8000-000000000001", "not-a-job", 1),
		strings.Replace(readerTestKey, "research", "../research", 1), readerTestKey + "/extra",
		strings.ReplaceAll(readerTestKey, "-4000-", "-7000-"),
		strings.ReplaceAll(readerTestKey, "-8000-", "-c000-"),
	} {
		if isSharedLogKey(key) {
			t.Errorf("broadened log grant for %q", key)
		}
	}
}

func testPOSIXACL(uid uint32, owner, mask uint16) []byte {
	data := make([]byte, 44)
	binary.LittleEndian.PutUint32(data, 2)
	entries := []struct {
		tag, perm uint16
		id        uint32
	}{{1, owner, ^uint32(0)}, {2, 5, uid}, {4, 0, ^uint32(0)}, {16, mask, ^uint32(0)}, {32, 0, ^uint32(0)}}
	for index, entry := range entries {
		offset := 4 + index*8
		binary.LittleEndian.PutUint16(data[offset:], entry.tag)
		binary.LittleEndian.PutUint16(data[offset+2:], entry.perm)
		binary.LittleEndian.PutUint32(data[offset+4:], entry.id)
	}
	return data
}

func TestLogACLPrivateFileHasNoEffectiveReaderGrant(t *testing.T) {
	if err := validatePOSIXLogACL(testPOSIXACL(21901, 6, 0), 21901, 6, 0); err != nil {
		t.Fatal(err)
	}
	if err := validatePOSIXLogACL(testPOSIXACL(21901, 6, 0), 21901, 6, 4); err == nil {
		t.Fatal("accepted masked private file as readable")
	}
}

func TestPOSIXLogACLRejectsEveryUnapprovedGrant(t *testing.T) {
	valid := testPOSIXACL(21901, 7, 5)
	if err := validatePOSIXLogACL(valid, 21901, 7, 5); err != nil {
		t.Fatal(err)
	}
	if err := validatePOSIXLogACL(testPOSIXACL(21902, 7, 5), 21901, 7, 5); err == nil {
		t.Fatal("accepted another named reader")
	}
	for index := range valid {
		changed := append([]byte(nil), valid...)
		changed[index] ^= 1
		if err := validatePOSIXLogACL(changed, 21901, 7, 5); err == nil {
			t.Fatalf("accepted changed ACL byte %d", index)
		}
	}
	if err := validatePOSIXLogACL(testPOSIXACL(21901, 7, 0), 21901, 7, 5); err == nil {
		t.Fatal("accepted masked existing directory")
	}
}

func nfsACLFixtures(t *testing.T) map[string][]byte {
	t.Helper()
	data, err := os.ReadFile("testdata/log-reader-nfs4.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		ReaderUID uint32 `json:"reader_uid"`
		Objects   map[string]struct {
			Xattrs map[string]struct {
				Base64 string `json:"base64"`
			} `json:"xattrs"`
		} `json:"objects"`
	}
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.ReaderUID != 21901 {
		t.Fatal("fixture reader identity changed")
	}
	result := make(map[string][]byte)
	for name, object := range fixture.Objects {
		encoded, decodeErr := base64.StdEncoding.DecodeString(object.Xattrs["system.nfs4_acl"].Base64)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		result[name] = encoded
	}
	return result
}

func TestNFSLogACLMatchesObservedLabInheritance(t *testing.T) {
	fixtures := nfsACLFixtures(t)
	for name, options := range map[string]struct{ directory, shared bool }{".": {true, true}, "traversable": {true, true}, "readable": {false, true}, "masked": {false, false}, "masked-dir": {true, false}} {
		if err := validateNFSLogACL(fixtures[name], 21901, options.directory, options.shared); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := validateNFSLogACL(fixtures["masked-dir"], 21901, true, true); err == nil {
		t.Fatal("accepted masked existing NFS directory")
	}
	if err := validateNFSLogACL(fixtures["readable"], 21902, false, true); err == nil {
		t.Fatal("accepted different numeric reader")
	}
}

func TestNFSLogACLRejectsAmbiguousOrBroaderPolicies(t *testing.T) {
	valid := nfsACLFixtures(t)["."]
	for index := range valid {
		changed := append([]byte(nil), valid...)
		changed[index] ^= 0x80
		if err := validateNFSLogACL(changed, 21901, true, true); err == nil {
			t.Fatalf("accepted changed NFS ACL byte %d", index)
		}
	}
	for _, data := range [][]byte{nil, valid[:len(valid)-1], append(append([]byte(nil), valid...), 0), make([]byte, maximumLogACLBytes+1)} {
		if err := validateNFSLogACL(data, 21901, true, true); err == nil {
			t.Fatal("accepted unbounded or truncated NFS ACL")
		}
	}
	// The only supported pattern is exact ordered allow entries. Deny, audit,
	// named/domain aliases, extra rights and unfamiliar inheritance all fail.
	for _, offset := range []int{4, 8, 12, 20} {
		changed := append([]byte(nil), valid...)
		changed[offset+3] ^= 1
		if err := validateNFSLogACL(changed, 21901, true, true); err == nil {
			t.Fatal("accepted unsupported NFS ACL semantics")
		}
	}
}
