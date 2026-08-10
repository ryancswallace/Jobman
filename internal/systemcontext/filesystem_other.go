//go:build !darwin && !linux

package systemcontext

import "github.com/ryancswallace/jobman/diagnostic"

func observeFilesystem(string) *diagnostic.FilesystemCapacity { return nil }
