//go:build windows

package agent

import "os"

func processSignal(_ *os.ProcessState) string { return "" }
