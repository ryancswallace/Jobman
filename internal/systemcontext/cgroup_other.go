//go:build !linux

package systemcontext

import "github.com/ryancswallace/jobman/diagnostic"

func observeLinuxCgroup() (context *diagnostic.LinuxCgroupContext, source string) { return nil, "" }
