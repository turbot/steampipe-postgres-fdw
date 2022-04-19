package sql

import (
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
)

type getSQLForTableTest struct {
	table       string
	tableSchema *proto.TableSchema
	localSchema string
	serverName  string
	expected    string
}

var testCasesgetSQLForTable = map[string]getSQLForTableTest{
	"no descriptions": {
		table: "t1",
		tableSchema: &proto.TableSchema{
			Columns: []*proto.ColumnDefinition{
				{
					Name: "c1",
					Type: proto.ColumnType_STRING,
				},
				{
					Name: "c2",
					Type: proto.ColumnType_STRING,
				},
			},
		},
		localSchema: "aws",
		serverName:  "steampipe",
		expected: `create foreign table "aws"."t1"
(
  "c1" text,
  "c2" text
)
server "steampipe" OPTIONS (table $steampipe_escape$t1$steampipe_escape$)`},
	"quotes in names": {
		table: "t1",
		tableSchema: &proto.TableSchema{
			Columns: []*proto.ColumnDefinition{
				{
					Name: `"c1"`,
					Type: proto.ColumnType_STRING,
				},
				{
					Name: `c2 "is" partially quoted`,
					Type: proto.ColumnType_STRING,
				},
			},
		},
		localSchema: "aws",
		serverName:  "steampipe",
		expected: `create foreign table "aws"."t1"
(
  """c1""" text,
  "c2 ""is"" partially quoted" text
)
server "steampipe" OPTIONS (table $steampipe_escape$t1$steampipe_escape$)`},
}

func TestGetSQLForTable(t *testing.T) {
	for name, test := range testCasesgetSQLForTable {

		result, err := GetSQLForTable(test.table, test.tableSchema, test.localSchema, test.serverName)
		if err != nil {
			if test.expected != "ERROR" {
				t.Errorf(`Test: '%s'' FAILED : unexpected error %v`, name, err)
			}
			continue
		}

		if test.expected != result {
			t.Errorf("Test: '%s' FAILED : expected \n%s\ngot\n%s", name, test.expected, result)
		}
	}
}

/*
test cases
invalid table
invalid columns
list error
hydrate error

*/
