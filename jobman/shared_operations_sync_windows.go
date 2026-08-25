//go:build windows

package jobman

// Windows does not support flushing a directory handle opened through os.Open.
// The operation file itself is flushed before the atomic rename.
func syncSharedOperationDirectory(string) error { return nil }
