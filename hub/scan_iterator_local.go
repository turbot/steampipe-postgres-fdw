package hub

import (
	"context"
	"log"

	"github.com/turbot/steampipe-plugin-sdk/v5/anywhere"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/row_stream"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
)

type scanIteratorLocal struct {
	scanIteratorBase
	pluginName string
}

func newScanIteratorLocal(hub Hub, connectionName, table, pluginName string, connectionLimitMap map[string]int64, qualMap map[string]*proto.Quals, columns []string, limit int64, traceCtx *telemetry.TraceCtx) *scanIteratorLocal {
	return &scanIteratorLocal{
		scanIteratorBase: newBaseScanIterator(hub, connectionName, table, connectionLimitMap, qualMap, columns, limit, traceCtx),
		pluginName:       pluginName,
	}
}

// GetPluginName implements Iterator
func (i *scanIteratorLocal) GetPluginName() string {
	return i.pluginName
}

// execute implements executor
func (i *scanIteratorLocal) execute(req *proto.ExecuteRequest) (row_stream.Receiver, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	// create a local stream
	stream := anywhere.NewLocalPluginStream(ctx)

	plugin := i.hub.(*HubLocal).plugin
	log.Printf("[INFO] StartScan for table: %s, cache enabled: %v, iterator %p, %d quals (%s)", i.table, req.CacheEnabled, i, len(i.queryContext.Quals), i.callId)
	plugin.CallExecuteAsync(req, stream)

	return stream, ctx, cancel, nil
}
