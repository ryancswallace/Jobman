package artifact

// FilesystemReader exposes verified reads without loading producer-only policy.
// Operating-system permissions and the caller's authorized manifest still govern
// access; a reader cannot write through this API.
type FilesystemReader struct {
	store *FilesystemStore
}

// NewFilesystemReader validates a logical mapping for a read-only consumer.
func NewFilesystemReader(name string, version int64, root string) (*FilesystemReader, error) {
	store, err := validateFilesystemMapping(name, version, root)
	if err != nil {
		return nil, err
	}
	return &FilesystemReader{store: store}, nil
}

// ReadVerified reads an immutable object matching its manifest size and digest.
func (reader *FilesystemReader) ReadVerified(key string, length int64, digest string) ([]byte, error) {
	return reader.store.ReadVerified(key, length, digest)
}
