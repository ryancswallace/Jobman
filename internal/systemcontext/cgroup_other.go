//go:build !linux

package systemcontext

import "github.com/ryancswallace/jobman/diagnostic"

func observeLinuxCgroup() (*diagnostic.LinuxCgroupContext, string) { return nil, "" }
