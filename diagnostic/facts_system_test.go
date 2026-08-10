package diagnostic

import "testing"

func TestSystemContextValidation(t *testing.T) {
	t.Parallel()

	current := uint64(1024)
	oom := uint64(2)
	valid := SystemContext{
		Scope: SystemScopeCollectorHost,
		Filesystem: &FilesystemCapacity{
			Scope: SystemFilesystemScope, Source: SystemFilesystemStatfs,
			AvailableBytes: 50, TotalBytes: 100,
		},
		LinuxCgroup: &LinuxCgroupContext{
			Version: 2, MemoryCurrentBytes: &current,
			MemoryMaximum: &SystemLimit{Value: 4096}, CumulativeOOM: &oom,
			PIDsMaximum: &SystemLimit{Unlimited: true},
		},
		ContainerHint: "docker",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate(valid) error = %v", err)
	}

	tests := map[string]func(*SystemContext){
		"scope":            func(value *SystemContext) { value.Scope = "target" },
		"empty":            func(value *SystemContext) { value.Filesystem = nil; value.LinuxCgroup = nil; value.ContainerHint = "" },
		"filesystem scope": func(value *SystemContext) { value.Filesystem.Scope = "root" },
		"filesystem source": func(value *SystemContext) {
			value.Filesystem.Source = "df"
		},
		"filesystem total": func(value *SystemContext) { value.Filesystem.TotalBytes = 0 },
		"filesystem available": func(value *SystemContext) {
			value.Filesystem.AvailableBytes = value.Filesystem.TotalBytes + 1
		},
		"cgroup version": func(value *SystemContext) { value.LinuxCgroup.Version = 1 },
		"cgroup empty": func(value *SystemContext) {
			value.LinuxCgroup = &LinuxCgroupContext{Version: 2}
		},
		"unlimited value": func(value *SystemContext) {
			value.LinuxCgroup.MemoryMaximum = &SystemLimit{Value: 1, Unlimited: true}
		},
		"container": func(value *SystemContext) { value.ContainerHint = "podman" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			value := cloneSystemContext(valid)
			mutate(&value)
			if err := value.Validate(); err == nil {
				t.Fatalf("Validate(%#v) error = nil", value)
			}
		})
	}
}

func cloneSystemContext(value SystemContext) SystemContext {
	filesystem := *value.Filesystem
	cgroup := *value.LinuxCgroup
	value.Filesystem = &filesystem
	value.LinuxCgroup = &cgroup

	return value
}
