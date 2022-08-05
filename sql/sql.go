package sql

import (
	"fmt"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe/pkg/db/db_common"
)

func GetSQLForTable(table string, tableSchema *proto.TableSchema, localSchema string, serverName string) (string, error) {
	// escape everything
	serverName = db_common.PgEscapeName(serverName)
	localSchema = db_common.PgEscapeName(localSchema)
	escapedTableName := db_common.PgEscapeName(table)
	// we must escape differently for the option
	escapedTableString := db_common.PgEscapeString(table)

	var columnsString []string
	for i, c := range tableSchema.Columns {
		column := db_common.PgEscapeName(c.Name)
		t, err := sqlTypeForColumnType(c.Type)
		if err != nil {
			return "", err
		}
		trailing := ","
		if i+1 == len(tableSchema.Columns) {
			trailing = ""
		}

		columnsString = append(columnsString, fmt.Sprintf("%s %s%s", column, t, trailing))
	}

	sql := fmt.Sprintf(`create foreign table %s.%s
(
  %s
)
server %s OPTIONS (table %s)`,
		localSchema,
		escapedTableName,
		strings.Join(columnsString, "\n  "),
		serverName,
		escapedTableString)

	return sql, nil
}

func sqlTypeForColumnType(columnType proto.ColumnType) (string, error) {
	switch columnType {
	case proto.ColumnType_BOOL:
		return "bool", nil
	case proto.ColumnType_INT:
		return "bigint", nil
	case proto.ColumnType_DOUBLE:
		return "double precision", nil
	case proto.ColumnType_STRING:
		return "text", nil
	case proto.ColumnType_IPADDR, proto.ColumnType_INET:
		return "inet", nil
	case proto.ColumnType_CIDR:
		return "cidr", nil
	case proto.ColumnType_JSON:
		return "jsonb", nil
	case proto.ColumnType_DATETIME, proto.ColumnType_TIMESTAMP:
		return "timestamptz", nil
	case proto.ColumnType_LTREE:
		return "ltree", nil
	}
	return "", fmt.Errorf("unsupported column type %v", columnType)

}
