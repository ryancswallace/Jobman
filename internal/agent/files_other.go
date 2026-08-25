//go:build !linux && !darwin && !windows

package agent

import "errors"

func requireLocalFilesystem(string) error {
	return errors.New("agent state filesystem verification is unsupported on this platform")
}
