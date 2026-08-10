//go:build darwin || linux

package systemcontext

import "testing"

func TestObserveFilesystem(t *testing.T) {
	t.Parallel()

	capacity := observeFilesystem(t.TempDir())
	if capacity == nil || capacity.TotalBytes == 0 || capacity.AvailableBytes > capacity.TotalBytes {
		t.Fatalf("observeFilesystem() = %#v", capacity)
	}
	if observeFilesystem("") != nil || observeFilesystem(t.TempDir()+"/missing") != nil {
		t.Fatal("observeFilesystem(invalid path) returned an observation")
	}
}
