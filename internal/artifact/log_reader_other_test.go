//go:build !linux

package artifact

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfiguredLinuxLogReaderPolicyFailsOnUnsupportedPlatform(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, logReaderPolicyFilename), []byte(readerTestPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFilesystemStore("logs", 1, root); err == nil || !strings.Contains(err.Error(), "supported Linux filesystem") {
		t.Fatalf("unsupported policy: %v", err)
	}
}
