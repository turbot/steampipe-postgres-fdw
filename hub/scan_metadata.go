package hub

import (
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

type ScanMetadata struct {
	Id           int
	Connection   string
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
		"connection":    m.Connection,
		"table":         m.Table,
		"cache_hit":     m.CacheHit,
		"rows_fetched":  m.RowsFetched,
		"hydrate_calls": m.HydrateCalls,
		"start_time":    m.StartTime,
		"duration_ms":   m.Duration.Milliseconds(),
		"columns":       m.Columns,
		"quals":         grpc.QualMapToSerializableSlice(m.Quals),
	}

	if m.Limit == -1 {
		res["limit"] = nil
	} else {
		res["limit"] = m.Limit
	}
	return res
}
