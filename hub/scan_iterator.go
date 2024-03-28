package hub

import (
	"context"
	"log"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/row_stream"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe/pkg/query/queryresult"
	"github.com/turbot/steampipe/pkg/steampipeconfig"
	"golang.org/x/exp/maps"
)

// TODO think about when we reset status from complete to ready

type scanIterator struct {
	scanIteratorBase
	connectionPlugin *steampipeconfig.ConnectionPlugin
	hub              *RemoteHub
}

func newScanIterator(hub Hub, connectionPlugin *steampipeconfig.ConnectionPlugin, connectionName, table string, connectionLimitMap map[string]int64, qualMap map[string]*proto.Quals, columns []string, limit int64, traceCtx *telemetry.TraceCtx, queryTimestamp int64,sortOrder []*proto.SortColumn) *scanIterator {
	return &scanIterator{
		scanIteratorBase: newBaseScanIterator(hub, connectionName, table, connectionLimitMap, qualMap, columns, limit, traceCtx, queryTimestamp, sortOrder),
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

// GetScanMetadata returns the scan metadata for this iterator
// note: if this is an aggregator query, we will have a scan metadata for each connection
// we need to combine them into a single scan metadata object
func (i *scanIterator) GetScanMetadata() []queryresult.ScanMetadataRow {
	// if we have scan metadata, return it
	if len(i.scanMetadata) > 0 {
		return maps.Values(i.scanMetadata)
	}

	// if there is no scan metadata, add an empty one
	var res []queryresult.ScanMetadataRow
	for connection := range i.connectionLimitMap {
		res = append(res, i.newScanMetadata(connection, nil))
	}
	return res
}
