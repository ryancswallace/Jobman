package diagnostic

import "errors"

// SystemContext contains bounded point-in-time facts about the environment in
// which Jobman collected evidence. Cgroup counters are cumulative for the
// containing cgroup and never prove that one selected run caused an event.
type SystemContext struct {
	Scope         string              `json:"scope"`
	Filesystem    *FilesystemCapacity `json:"filesystem,omitempty"`
	LinuxCgroup   *LinuxCgroupContext `json:"linux_cgroup,omitempty"`
	ContainerHint string              `json:"container_hint,omitempty"`
}

// FilesystemCapacity describes the filesystem that contains Jobman's state.
type FilesystemCapacity struct {
	Scope          string `json:"scope"`
	Source         string `json:"source"`
	AvailableBytes uint64 `json:"available_bytes"`
	TotalBytes     uint64 `json:"total_bytes"`
}

// LinuxCgroupContext contains allowlisted cgroup-v2 counters and limits for
// Jobman's own cgroup at collection time. Target processes ordinarily inherit
// that cgroup, but the evidence does not claim per-run attribution.
type LinuxCgroupContext struct {
	Version            int          `json:"version"`
	MemoryCurrentBytes *uint64      `json:"memory_current_bytes,omitempty"`
	MemoryMaximum      *SystemLimit `json:"memory_maximum,omitempty"`
	CumulativeOOM      *uint64      `json:"cumulative_oom_events,omitempty"`
	CumulativeOOMKills *uint64      `json:"cumulative_oom_kill_events,omitempty"`
	PIDsCurrent        *uint64      `json:"pids_current,omitempty"`
	PIDsMaximum        *SystemLimit `json:"pids_maximum,omitempty"`
}

// SystemLimit is either a finite nonnegative value or explicitly unlimited.
type SystemLimit struct {
	Value     uint64 `json:"value,omitempty"`
	Unlimited bool   `json:"unlimited"`
}

const (
	// SystemScopeCollectorHost states that a system observation describes the
	// evidence collector's host at capture time, not one target process.
	SystemScopeCollectorHost = "collector_host_at_capture"
	// SystemFilesystemScope states that capacity describes the filesystem
	// containing Jobman's state directory without exposing its path.
	SystemFilesystemScope = "jobman_state_filesystem"
	// SystemFilesystemStatfs identifies the operating-system statfs source.
	SystemFilesystemStatfs = "statfs"
)

// Validate checks the controlled system-context shape.
func (context SystemContext) Validate() error {
	if context.Scope != SystemScopeCollectorHost ||
		(context.Filesystem == nil && context.LinuxCgroup == nil && context.ContainerHint == "") {
		return errors.New("validate system context: incomplete context")
	}
	if context.Filesystem != nil &&
		(context.Filesystem.Scope != SystemFilesystemScope ||
			context.Filesystem.Source != SystemFilesystemStatfs ||
			context.Filesystem.TotalBytes == 0 ||
			context.Filesystem.AvailableBytes > context.Filesystem.TotalBytes) {
		return errors.New("validate system context: invalid filesystem capacity")
	}
	if context.LinuxCgroup != nil && !context.LinuxCgroup.valid() {
		return errors.New("validate system context: invalid Linux cgroup context")
	}
	switch context.ContainerHint {
	case "", "docker", "kubernetes", "containerd", "other":
	default:
		return errors.New("validate system context: invalid container hint")
	}

	return nil
}

func (context LinuxCgroupContext) valid() bool {
	if context.Version != 2 || context.MemoryCurrentBytes == nil && context.MemoryMaximum == nil &&
		context.CumulativeOOM == nil && context.CumulativeOOMKills == nil &&
		context.PIDsCurrent == nil && context.PIDsMaximum == nil {
		return false
	}
	for _, limit := range []*SystemLimit{context.MemoryMaximum, context.PIDsMaximum} {
		if limit != nil && limit.Unlimited && limit.Value != 0 {
			return false
		}
	}

	return true
}
