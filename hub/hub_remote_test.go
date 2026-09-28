package hub

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbot/pipe-fittings/v2/app_specific"
	"github.com/turbot/steampipe/v2/pkg/steampipeconfig"
)

// malformed JSON connection config: a config load failure that stays a hard
// error regardless of how duplicate/invalid-name connections are tolerated.
const malformedConnectionConfigJSON = `{"connection": {"chaos": {"plugin": "chaos"`

// writeInstallDir sets app_specific.InstallDir to a fresh temp dir containing a
// config/ folder with the given connection config files, and restores the
// previous InstallDir on test cleanup.
func writeInstallDir(t *testing.T, files map[string]string) {
	t.Helper()
	dir := t.TempDir()
	configDir := filepath.Join(dir, "config")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("failed to create config dir: %v", err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(configDir, name), []byte(content), 0644); err != nil {
			t.Fatalf("failed to write %s: %v", name, err)
		}
	}

	prevInstallDir := app_specific.InstallDir
	app_specific.InstallDir = dir
	t.Cleanup(func() { app_specific.InstallDir = prevInstallDir })
}

// resetGlobalConfig saves and restores steampipeconfig.GlobalConfig around a test,
// since it is process-global state shared with every other hub test.
func resetGlobalConfig(t *testing.T) {
	t.Helper()
	prev := steampipeconfig.GlobalConfig
	t.Cleanup(func() { steampipeconfig.GlobalConfig = prev })
}

func TestLoadConnectionConfig_FirstLoadFails_GlobalConfigStaysNonNil(t *testing.T) {
	resetGlobalConfig(t)
	steampipeconfig.GlobalConfig = nil

	writeInstallDir(t, map[string]string{
		"broken.json": malformedConnectionConfigJSON,
	})

	h := &RemoteHub{}
	_, err := h.LoadConnectionConfig()
	if err == nil || !strings.Contains(err.Error(), "Unclosed object") {
		t.Fatalf("expected an unclosed-JSON-object error, got: %v", err)
	}

	if steampipeconfig.GlobalConfig == nil {
		t.Fatal("GlobalConfig is nil after a failed load - this is the nil pointer panic waiting to happen")
	}
}

func TestLoadConnectionConfig_ReloadFails_KeepsLastGoodConfig(t *testing.T) {
	resetGlobalConfig(t)
	goodConfig := steampipeconfig.NewSteampipeConfig("")
	steampipeconfig.GlobalConfig = goodConfig

	writeInstallDir(t, map[string]string{
		"broken.json": malformedConnectionConfigJSON,
	})

	h := &RemoteHub{}
	_, err := h.LoadConnectionConfig()
	if err == nil || !strings.Contains(err.Error(), "Unclosed object") {
		t.Fatalf("expected an unclosed-JSON-object error, got: %v", err)
	}

	if steampipeconfig.GlobalConfig != goodConfig {
		t.Fatal("a failed reload discarded the last successfully loaded config")
	}
}
