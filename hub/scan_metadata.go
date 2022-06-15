package hub

import (
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v3/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
)

type ScanMetadata struct {
	Id           int
	Table        string
	CacheHit     bool
	RowsFetched  int64
	HydrateCalls int64
	Columns      []string
	Quals        map[string]*proto.Quals
	Limit        int64
	StartTime    time.Time
	Duration     time.Duration
}

// AsResultRow returns the ScanMetadata as a map[string]interface which can be returned as a query result
func (m ScanMetadata) AsResultRow() map[string]interface{} {
	res := map[string]interface{}{
		"id":            m.Id,
		"table":         m.Table,
		"cache_hit":     m.CacheHit,
		"rows_fetched":  m.RowsFetched,
		"hydrate_calls": m.HydrateCalls,
		"start_time":    m.StartTime,
		"duration":      m.Duration.Seconds(),
		"columns":       m.Columns,
	}
	if m.Limit != -1 {
		res["limit"] = m.Limit
	}
	if len(m.Quals) > 0 {
		// ignore error
		res["quals"], _ = grpc.QualMapToJSONString(m.Quals)
	}
	return res
}
