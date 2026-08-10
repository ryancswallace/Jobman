//go:build linux

package systemcontext

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestObserveLinuxCgroupFromFixture(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	proc := filepath.Join(root, "proc-self-cgroup")
	cgroupRoot := filepath.Join(root, "cgroup")
	group := filepath.Join(cgroupRoot, "docker", "jobman")
	if err := os.MkdirAll(group, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFixture := func(path, value string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeFixture(proc, "0::/docker/jobman\n")
	writeFixture(filepath.Join(group, "memory.current"), "1024\n")
	writeFixture(filepath.Join(group, "memory.max"), "4096\n")
	writeFixture(filepath.Join(group, "memory.events"), strings.Join([]string{"low 1", "oom 2", "oom_kill 1", ""}, "\n"))
	writeFixture(filepath.Join(group, "pids.current"), "3\n")
	writeFixture(filepath.Join(group, "pids.max"), "max\n")

	context, hint := observeLinuxCgroupFrom(proc, cgroupRoot, filepath.Join(root, "no-marker"))
	if context == nil || context.MemoryCurrentBytes == nil || *context.MemoryCurrentBytes != 1024 ||
		context.MemoryMaximum == nil || context.MemoryMaximum.Value != 4096 ||
		context.CumulativeOOM == nil || *context.CumulativeOOM != 2 ||
		context.CumulativeOOMKills == nil || *context.CumulativeOOMKills != 1 ||
		context.PIDsCurrent == nil || *context.PIDsCurrent != 3 ||
		context.PIDsMaximum == nil || !context.PIDsMaximum.Unlimited || hint != "docker" {
		t.Fatalf("observeLinuxCgroupFrom() = %#v, %q", context, hint)
	}

	writeFixture(proc, "invalid\n")
	marker := filepath.Join(root, "container-marker")
	writeFixture(marker, "")
	if context, hint = observeLinuxCgroupFrom(proc, cgroupRoot, marker); context != nil || hint != "docker" {
		t.Fatalf("observeLinuxCgroupFrom(invalid) = %#v, %q", context, hint)
	}
	if context, _ = observeLinuxCgroupFrom(filepath.Join(root, "missing"), cgroupRoot, marker); context != nil {
		t.Fatalf("observeLinuxCgroupFrom(missing) = %#v", context)
	}
}

func TestParseCgroupV2Path(t *testing.T) {
	t.Parallel()

	path, found := parseCgroupV2Path("7:cpu:/legacy\n0::/system.slice/jobman.service\n")
	if !found || path != "/system.slice/jobman.service" {
		t.Fatalf("parseCgroupV2Path() = %q, %v", path, found)
	}
	for _, invalid := range []string{"", "0:cpu:/group", "0::relative", "bad"} {
		if _, found := parseCgroupV2Path(invalid); found {
			t.Fatalf("parseCgroupV2Path(%q) found a path", invalid)
		}
	}
}

func TestConfinedCgroupPath(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "cgroup")
	path, err := confinedCgroupPath(root, "/system.slice/jobman.service")
	if err != nil || path != filepath.Join(root, "system.slice", "jobman.service") {
		t.Fatalf("confinedCgroupPath() = %q, %v", path, err)
	}
	if _, err := confinedCgroupPath(root, "/../../escape"); err == nil {
		t.Fatal("confinedCgroupPath(escape) error = nil")
	}
}

func TestParseMemoryEventsAndContainerHint(t *testing.T) {
	t.Parallel()

	oom, kills := parseMemoryEvents(strings.Join(
		[]string{"low 1", "oom 2", "oom_kill 3", "oom_group_kill invalid", ""}, "\n",
	))
	if oom == nil || *oom != 2 || kills == nil || *kills != 3 {
		t.Fatalf("parseMemoryEvents() = %v, %v", oom, kills)
	}
	if hint := containerHint("/kubepods.slice/pod", "docker"); hint != "kubernetes" {
		t.Fatalf("containerHint(kubernetes) = %q", hint)
	}
	if hint := containerHint("/plain", "docker"); hint != "docker" {
		t.Fatalf("containerHint(marker) = %q", hint)
	}
	if hint := containerHint("/containerd/task", ""); hint != "containerd" {
		t.Fatalf("containerHint(containerd) = %q", hint)
	}
	if hint := containerHint("/plain", ""); hint != "" {
		t.Fatalf("containerHint(plain) = %q", hint)
	}
}

func TestCgroupFactReadersAreBoundedAndStrict(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	write := func(name, value string) string {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}

		return path
	}
	if value := readUint(write("uint", "42\n")); value == nil || *value != 42 {
		t.Fatalf("readUint(valid) = %v", value)
	}
	if readUint(write("bad-uint", "no")) != nil || readUint(filepath.Join(root, "missing")) != nil {
		t.Fatal("readUint accepted an invalid value")
	}
	if limit := readLimit(write("finite", "7")); limit == nil || limit.Value != 7 || limit.Unlimited {
		t.Fatalf("readLimit(finite) = %#v", limit)
	}
	if limit := readLimit(write("unlimited", "max")); limit == nil || !limit.Unlimited {
		t.Fatalf("readLimit(max) = %#v", limit)
	}
	if readLimit(write("bad-limit", "none")) != nil || readLimit(filepath.Join(root, "missing-limit")) != nil {
		t.Fatal("readLimit accepted an invalid value")
	}
	if _, err := readBounded(write("large", strings.Repeat("x", 5)), 4); err == nil {
		t.Fatal("readBounded(large) error = nil")
	}
	if _, err := readBounded(root, 4); err == nil {
		t.Fatal("readBounded(directory) error = nil")
	}
	if oom, kills := parseMemoryEvents("oom invalid\n"); oom != nil || kills != nil {
		t.Fatalf("parseMemoryEvents(invalid) = %v, %v", oom, kills)
	}
}
