//go:build !windows

package runtimeenv

import (
	"os"
	"strconv"
)

func currentProcessUser() string {
	return strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid())
}
