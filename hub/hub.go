package hub

import (
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-postgres-fdw/types"
)

// TODO check which can be non-imported
type Hub interface {
	GetConnectionConfigByName(string) *proto.ConnectionConfig
	LoadConnectionConfig() (bool, error)
	GetSchema(remoteSchema string, localSchema string) (*proto.Schema, error)
	GetIterator(columns []string, quals *proto.Quals, unhandledRestrictions int, limit int64, opts types.Options, queryTimestamp int64) (Iterator, error)
	GetRelSize(columns []string, quals []*proto.Qual, opts types.Options) (types.RelSize, error)
	GetPathKeys(opts types.Options) ([]types.PathKey, error)
	Explain(columns []string, quals []*proto.Qual, sortKeys []string, verbose bool, opts types.Options) ([]string, error)
	ApplySetting(key string, value string) error
	GetSettingsSchema() map[string]*proto.TableSchema
	GetLegacySettingsSchema() map[string]*proto.TableSchema
	StartScan(i Iterator) error
	RemoveIterator(iterator Iterator)
	EndScan(iter Iterator, limit int64)
	AddScanMetadata(iter Iterator)
	Abort()
	Close()
	ProcessImportForeignSchemaOptions(opts types.Options, connectionName string) error
	HandleLegacyCacheCommand(command string) error
	ValidateCacheCommand(command string) error
	cacheTTL(name string) time.Duration
	cacheEnabled(name string) bool
	GetSortableFields(table, connection string) map[string]proto.SortOrder
}
