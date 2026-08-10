//go:build linux

package systemcontext

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ryancswallace/jobman/diagnostic"
)

const (
	procSelfCgroup  = "/proc/self/cgroup"
	cgroupV2Root    = "/sys/fs/cgroup"
	maximumFactSize = 4096
)

func observeLinuxCgroup() (observed *diagnostic.LinuxCgroupContext, hint string) {
	return observeLinuxCgroupFrom(procSelfCgroup, cgroupV2Root, "/.dockerenv")
}

func observeLinuxCgroupFrom(procPath, root, markerPath string) (observed *diagnostic.LinuxCgroupContext, hint string) {
	encoded, err := readBounded(procPath, 64*1024)
	if err != nil {
		return nil, containerMarkerHint(markerPath)
	}
	relative, found := parseCgroupV2Path(string(encoded))
	if !found {
		return nil, containerMarkerHint(markerPath)
	}
	directory, err := confinedCgroupPath(root, relative)
	if err != nil {
		return nil, containerHint(relative, containerMarkerHint(markerPath))
	}
	cgroup := &diagnostic.LinuxCgroupContext{Version: 2}
	cgroup.MemoryCurrentBytes = readUint(filepath.Join(directory, "memory.current"))
	cgroup.MemoryMaximum = readLimit(filepath.Join(directory, "memory.max"))
	if events, readErr := readBounded(filepath.Join(directory, "memory.events"), maximumFactSize); readErr == nil {
		cgroup.CumulativeOOM, cgroup.CumulativeOOMKills = parseMemoryEvents(string(events))
	}
	cgroup.PIDsCurrent = readUint(filepath.Join(directory, "pids.current"))
	cgroup.PIDsMaximum = readLimit(filepath.Join(directory, "pids.max"))
	if (diagnostic.SystemContext{
		Scope:       diagnostic.SystemScopeCollectorHost,
		LinuxCgroup: cgroup,
	}).Validate() != nil {
		cgroup = nil
	}

	return cgroup, containerHint(relative, containerMarkerHint(markerPath))
}

func readBounded(path string, maximum int64) ([]byte, error) {
	file, err := os.Open(path) // #nosec G304 -- every production caller supplies a fixed allowlisted system path.
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	encoded, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(encoded)) > maximum {
		return nil, errors.New("system fact exceeds limit")
	}

	return encoded, nil
}

func parseCgroupV2Path(encoded string) (string, bool) {
	scanner := bufio.NewScanner(strings.NewReader(encoded))
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), ":", 3)
		if len(parts) == 3 && parts[0] == "0" && parts[1] == "" && strings.HasPrefix(parts[2], "/") {
			return parts[2], true
		}
	}

	return "", false
}

func confinedCgroupPath(root, relative string) (string, error) {
	root = filepath.Clean(root)
	candidate := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(relative, "/")))
	within, err := filepath.Rel(root, candidate)
	if err != nil || within == ".." || strings.HasPrefix(within, ".."+string(filepath.Separator)) {
		return "", errors.New("cgroup path escapes root")
	}

	return candidate, nil
}

func readUint(path string) *uint64 {
	encoded, err := readBounded(path, maximumFactSize)
	if err != nil {
		return nil
	}
	value, err := strconv.ParseUint(strings.TrimSpace(string(encoded)), 10, 64)
	if err != nil {
		return nil
	}

	return &value
}

func readLimit(path string) *diagnostic.SystemLimit {
	encoded, err := readBounded(path, maximumFactSize)
	if err != nil {
		return nil
	}
	value := strings.TrimSpace(string(encoded))
	if value == "max" {
		return &diagnostic.SystemLimit{Unlimited: true}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil
	}

	return &diagnostic.SystemLimit{Value: parsed}
}

func parseMemoryEvents(encoded string) (oom, kills *uint64) {
	values := make(map[string]uint64)
	scanner := bufio.NewScanner(strings.NewReader(encoded))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || fields[0] != "oom" && fields[0] != "oom_kill" {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err == nil {
			values[fields[0]] = value
		}
	}

	return mapValue(values, "oom"), mapValue(values, "oom_kill")
}

func mapValue(values map[string]uint64, key string) *uint64 {
	value, found := values[key]
	if !found {
		return nil
	}

	return &value
}

func containerMarkerHint(path string) string {
	if _, err := os.Stat(path); err == nil {
		return "docker"
	}

	return ""
}

func containerHint(cgroupPath, marker string) string {
	lower := strings.ToLower(cgroupPath)
	switch {
	case strings.Contains(lower, "kubepods"):
		return "kubernetes"
	case strings.Contains(lower, "containerd"):
		return "containerd"
	case strings.Contains(lower, "docker"):
		return "docker"
	default:
		return marker
	}
}
