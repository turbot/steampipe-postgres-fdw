package hub

import (
	"testing"

	"github.com/turbot/steampipe-postgres-fdw/v2/settings"
	"github.com/turbot/steampipe/v2/pkg/constants"
)

func newCacheTestHub() *hubBase {
	return &hubBase{
		cacheSettings: settings.NewCacheSettings(func(string) error { return nil }, true),
	}
}

func TestHandleLegacyCacheCommandOff(t *testing.T) {
	h := newCacheTestHub()

	if err := h.HandleLegacyCacheCommand(constants.LegacyCommandCacheOff); err != nil {
		t.Fatalf("cache_off returned error: %v", err)
	}

	enabled := h.cacheSettings.ClientCacheEnabled
	if enabled == nil || *enabled {
		t.Fatalf("expected client cache disabled after cache_off, got %v", enabled)
	}
}

func TestHandleLegacyCacheCommandOn(t *testing.T) {
	h := newCacheTestHub()

	if err := h.HandleLegacyCacheCommand(constants.LegacyCommandCacheOn); err != nil {
		t.Fatalf("cache_on returned error: %v", err)
	}

	enabled := h.cacheSettings.ClientCacheEnabled
	if enabled == nil || !*enabled {
		t.Fatalf("expected client cache enabled after cache_on, got %v", enabled)
	}
}
