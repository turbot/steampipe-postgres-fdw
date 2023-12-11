package hub

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/row_stream"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe/pkg/steampipeconfig"
	"log"
)

// TODO think about when we reset status from complete to ready

type scanIterator struct {
	scanIteratorBase
	connectionPlugin *steampipeconfig.ConnectionPlugin
	hub              *RemoteHub
}

func newScanIterator(hub Hub, connectionPlugin *steampipeconfig.ConnectionPlugin, connectionName, table string, connectionLimitMap map[string]int64, qualMap map[string]*proto.Quals, columns []string, limit int64, traceCtx *telemetry.TraceCtx) *scanIterator {
	return &scanIterator{
		scanIteratorBase: newBaseScanIterator(hub, connectionName, table, connectionLimitMap, qualMap, columns, limit, traceCtx),
		connectionPlugin: connectionPlugin,
	}
}

// GetPluginName implements Iterator
func (i *scanIterator) GetPluginName() string {
	return i.connectionPlugin.PluginName
}

// execute implements executor
func (i *scanIterator) execute(req *proto.ExecuteRequest) (row_stream.Receiver, context.Context, context.CancelFunc, error) {
	log.Printf("[INFO] StartScan for table: %s, cache enabled: %v, iterator %p, %d quals (%s)", i.table, req.CacheEnabled, i, len(i.queryContext.Quals), i.callId)
	stream, ctx, cancel, err := i.connectionPlugin.PluginClient.Execute(req)
	// format GRPC errors
	err = grpc.HandleGrpcError(err, i.connectionPlugin.PluginName, "Execute")
	if err != nil {
		return nil, nil, nil, err
	}
	return stream, ctx, cancel, nil
}
