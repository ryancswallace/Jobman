//go:build !linux

package artifact

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

//nolint:nilnil // An absent opt-in policy preserves private defaults.
func loadLogReaderPolicy(root, _ string, _ int64) (*logReaderPolicy, error) {
	_, err := os.Lstat(filepath.Join(root, logReaderPolicyFilename))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return nil, errors.New("configured log reader ACL policy requires a supported Linux filesystem")
}

func (*FilesystemStore) putReaderObject(string, io.Reader, int64, int64, bool) (Object, error) {
	return Object{}, errors.New("configured log reader ACL policy requires a supported Linux filesystem")
}
