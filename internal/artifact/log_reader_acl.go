package artifact

import (
	"encoding/binary"
	"errors"
	"strconv"
)

// Linux POSIX ACL xattr format: a little-endian version followed by fixed-size
// tag/permission/qualifier entries. Require exactly one named reader and no
// owning-group, other, additional user, or named-group grant.
func validatePOSIXLogACL(data []byte, uid uint32, owner, mask uint16) error {
	if len(data) != 44 || binary.LittleEndian.Uint32(data[:4]) != 2 {
		return errors.New("log reader requires an exact five-entry POSIX ACL")
	}
	entries := []struct {
		tag, permissions uint16
		qualifier        uint32
	}{
		{1, owner, ^uint32(0)},
		{2, 5, uid},
		{4, 0, ^uint32(0)},
		{16, mask, ^uint32(0)},
		{32, 0, ^uint32(0)},
	}
	for index, entry := range entries {
		offset := 4 + 8*index
		if binary.LittleEndian.Uint16(data[offset:]) != entry.tag ||
			binary.LittleEndian.Uint16(data[offset+2:]) != entry.permissions ||
			binary.LittleEndian.Uint32(data[offset+4:]) != entry.qualifier {
			return errors.New("log reader POSIX ACL differs from the configured grant")
		}
	}
	return nil
}

type logACE struct {
	kind, flags, permissions uint32
	principal                string
}

// The supported NFSv4 layout is the Linux server's allow-only projection of
// the exact POSIX policy. It is not a general NFSv4 ACL evaluator: deny/audit,
// reordered, ambiguous, domain-mapped and unfamiliar inheritance forms fail.
func validateNFSLogACL(data []byte, uid uint32, directory, shared bool) error {
	entries, err := decodeLogNFSACL(data)
	if err != nil {
		return err
	}
	want := 4
	if directory {
		want = 8
	}
	if len(entries) != want {
		return errors.New("log reader NFSv4 ACL has unsupported entries")
	}
	owner, reader := uint32(6), uint32(0)
	if directory {
		owner = 7
	}
	if shared {
		reader = 4
		if directory {
			reader = 5
		}
	}
	if err := validateNFSLogSection(entries[:4], uid, owner, reader, directory, 0); err != nil {
		return err
	}
	if directory {
		return validateNFSLogSection(entries[4:], uid, 7, 5, true, 11)
	}
	return nil
}

func validateNFSLogSection(entries []logACE, uid, owner, reader uint32, directory bool, flags uint32) error {
	principals := []string{"OWNER@", strconv.FormatUint(uint64(uid), 10), "GROUP@", "EVERYONE@"}
	permissions := []uint32{nfsLogPermissions(owner, true, directory), nfsLogPermissions(reader, false, directory), nfsLogPermissions(0, false, directory), nfsLogPermissions(0, false, directory)}
	for index, entry := range entries {
		validFlags := entry.flags == flags || index == 2 && entry.flags == flags|64
		if entry.kind != 0 || !validFlags || entry.principal != principals[index] || entry.permissions != permissions[index] {
			return errors.New("log reader NFSv4 ACL differs from the configured numeric reader policy")
		}
	}
	return nil
}

func nfsLogPermissions(mode uint32, owner, directory bool) uint32 {
	// Read attributes/ACL and synchronize are baseline server metadata rights;
	// they do not grant data reads, directory listing/traversal, or mutation.
	permissions := uint32(0x120080)
	if owner {
		permissions |= 0x40100
	} // Owner may write attributes and ACL.
	if mode&4 != 0 {
		permissions |= 1
	}
	if mode&2 != 0 {
		permissions |= 6 // Write and append data.
		if directory {
			permissions |= 0x40
		} // Delete child.
	}
	if mode&1 != 0 {
		permissions |= 0x20
	}
	return permissions
}

func decodeLogNFSACL(data []byte) ([]logACE, error) {
	if len(data) < 4 || len(data) > maximumLogACLBytes {
		return nil, errors.New("log reader NFSv4 ACL exceeds bounds")
	}
	count := binary.BigEndian.Uint32(data[:4])
	if count != 4 && count != 8 {
		return nil, errors.New("log reader NFSv4 ACL has unsupported entry count")
	}
	data = data[4:]
	entries := make([]logACE, 0, count)
	for range count {
		if len(data) < 16 {
			return nil, errors.New("log reader NFSv4 ACL is truncated")
		}
		length := binary.BigEndian.Uint32(data[12:16])
		if length == 0 || length > 32 {
			return nil, errors.New("log reader NFSv4 principal is invalid")
		}
		padded := (int(length) + 3) & ^3
		if len(data) < 16+padded {
			return nil, errors.New("log reader NFSv4 principal is truncated")
		}
		for _, padding := range data[16+int(length) : 16+padded] {
			if padding != 0 {
				return nil, errors.New("log reader NFSv4 principal has invalid padding")
			}
		}
		entries = append(entries, logACE{binary.BigEndian.Uint32(data[:4]), binary.BigEndian.Uint32(data[4:8]), binary.BigEndian.Uint32(data[8:12]), string(data[16 : 16+int(length)])})
		data = data[16+padded:]
	}
	if len(data) != 0 {
		return nil, errors.New("log reader NFSv4 ACL has trailing data")
	}
	return entries, nil
}
