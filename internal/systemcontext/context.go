// Package systemcontext observes a small, allowlisted set of point-in-time
// constraints on the host that is collecting diagnostic evidence.
package systemcontext

import "github.com/ryancswallace/jobman/diagnostic"

// Observe returns the system context that can be established without probing
// target processes or reading arbitrary host configuration. An incomplete
// result is intentionally returned when no supported observation is available.
func Observe(stateDir string) diagnostic.SystemContext {
	linuxCgroup, containerHint := observeLinuxCgroup()

	return diagnostic.SystemContext{
		Scope:         diagnostic.SystemScopeCollectorHost,
		Filesystem:    observeFilesystem(stateDir),
		LinuxCgroup:   linuxCgroup,
		ContainerHint: containerHint,
	}
}
