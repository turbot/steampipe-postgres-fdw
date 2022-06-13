package hub

import (
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
)

type ScanMetadata struct {
	Table        string
	CacheHit     bool
	RowsFetched  uint64
	HydrateCalls uint64
	Columns      []string
	Quals        map[string]*proto.Quals
	Limit        int64
}

// AsResultRow returns the ScanMetadata as a map[string]interface which can be returned as a query result
func (m ScanMetadata) AsResultRow() map[string]interface{} {
	res := map[string]interface{}{
		"table":         m.Table,
		"cache_hit":     m.CacheHit,
		"rows_fetched":  m.RowsFetched,
		"hydrate_calls": m.HydrateCalls,
		"columns":       m.Columns,
		"quals":         grpc.QualMapToString(m.Quals, false),
	}
	if m.Limit != -1 {
		res["limit"] = m.Limit
	}
	return res
}
