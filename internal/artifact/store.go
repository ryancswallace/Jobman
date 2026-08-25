package artifact

import "context"

// Store is the target-side boundary for one immutable artifact mapping. Store
// implementations own physical configuration; portable workloads refer only
// to Name and Version.
type Store interface {
	Name() string
	Version() int64
	Put(context.Context, string, []byte) (string, error)
	Read(context.Context, string, int64, string) ([]byte, error)
	Materialize(context.Context, string, string, int64, string) (Object, error)
	Publish(context.Context, string, string, int64) (Object, error)
}

// Put implements Store while preserving PutImmutable for compatibility.
func (store *FilesystemStore) Put(ctx context.Context, key string, contents []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	return store.PutImmutable(key, contents)
}

// Read implements Store while preserving ReadVerified for compatibility.
func (store *FilesystemStore) Read(
	ctx context.Context,
	key string,
	length int64,
	expectedDigest string,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return store.ReadVerified(key, length, expectedDigest)
}

// Materialize implements Store while preserving MaterializeFile for
// compatibility.
func (store *FilesystemStore) Materialize(
	ctx context.Context,
	key, destination string,
	maximumBytes int64,
	expectedDigest string,
) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}

	return store.MaterializeFile(key, destination, maximumBytes, expectedDigest)
}

// Publish implements Store while preserving PutFileImmutable for
// compatibility.
func (store *FilesystemStore) Publish(
	ctx context.Context,
	key, sourcePath string,
	maximumBytes int64,
) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}

	return store.PutFileImmutable(key, sourcePath, maximumBytes)
}
