package cache

import (
	"reflect"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe/steampipeconfig"
)

type cacheSetRequest struct {
	connection string
	table      string
	qualMap    map[string]*proto.Quals
	columns    []string
	result     *QueryResult
}
type cacheGetRequest struct {
	connection string
	table      string
	qualMap    map[string]*proto.Quals
	columns    []string
	result     *QueryResult
}

type CacheTest struct {
	set      []cacheSetRequest
	get      cacheGetRequest
	expected *QueryResult
}

// quals
var quals1 = []*proto.Qual{{
	FieldName: "c1",
	Operator:  &proto.Qual_StringValue{StringValue: "="},
	Value:     &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "test"}},
}}
var qualMap1 = map[string]*proto.Quals{"c1": {Quals: quals1}}

var quals2 = []*proto.Qual{{
	FieldName: "c1",
	Operator:  &proto.Qual_StringValue{StringValue: "="},
	Value:     &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "test2"}},
}}
var qualMap2 = map[string]*proto.Quals{"c1": {Quals: quals2}}

var testCasesCacheTest = map[string]CacheTest{
	"single entry no quals same columns": {
		set: []cacheSetRequest{
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c1"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val"}},
				},
			},
		},
		get: cacheGetRequest{
			connection: "connection_a",
			table:      "t1",
			columns:    []string{"c1"},
		},
		expected: &QueryResult{
			[]map[string]interface{}{
				{"c1": "c1_val"},
			},
		},
	},
	"single entry, same quals, same columns": {
		set: []cacheSetRequest{
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c1"},
				qualMap:    qualMap1,
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val"}},
				},
			},
		},
		get: cacheGetRequest{
			connection: "connection_a",
			table:      "t1",
			qualMap:    qualMap1,
			columns:    []string{"c1"},
		},
		expected: &QueryResult{
			[]map[string]interface{}{
				{"c1": "c1_val"},
			},
		},
	},
	"multiple connections - request first": {
		set: []cacheSetRequest{
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c1"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val_a"}},
				},
			},
			{
				connection: "connection_b",
				table:      "t1",
				columns:    []string{"c1"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val_b"}},
				},
			},
		},
		get: cacheGetRequest{
			connection: "connection_a",
			table:      "t1",
			columns:    []string{"c1"},
		},
		expected: &QueryResult{
			[]map[string]interface{}{
				{"c1": "c1_val_a"},
			},
		},
	},
	"multiple connections - request second": {
		set: []cacheSetRequest{
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c1"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val_a"}},
				},
			},
			{
				connection: "connection_b",
				table:      "t1",
				columns:    []string{"c1"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val_b"}},
				},
			},
		},
		get: cacheGetRequest{
			connection: "connection_b",
			table:      "t1",
			columns:    []string{"c1"},
		},
		expected: &QueryResult{
			[]map[string]interface{}{
				{"c1": "c1_val_b"},
			},
		},
	},
	"multiple entries no quals matching columns": {
		set: []cacheSetRequest{
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c1"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val"}},
				},
			},
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c2", "c3"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c2": "c2_val"},
						{"c3": "c23val"},
					},
				},
			},
		},
		get: cacheGetRequest{
			connection: "connection_a",
			table:      "t1",
			columns:    []string{"c1"},
		},
		expected: &QueryResult{
			[]map[string]interface{}{
				{"c1": "c1_val"}},
		},
	},
	"multiple entries no quals subset of columns": {
		set: []cacheSetRequest{
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c1"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val"}},
				},
			},
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c2", "c3"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c2": "c2_val"},
						{"c3": "c23val"},
					},
				},
			},
		},
		get: cacheGetRequest{
			connection: "connection_a",
			table:      "t1",
			columns:    []string{"c2"},
		},
		expected: &QueryResult{
			[]map[string]interface{}{
				{"c2": "c2_val"},
				{"c3": "c23val"},
			},
		},
	},
	"single entry, different quals, same columns - CACHE MISS": {
		set: []cacheSetRequest{
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c1"},
				qualMap:    qualMap1,
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val"}},
				},
			},
		},
		get: cacheGetRequest{
			connection: "connection_a",
			table:      "t1",
			qualMap:    qualMap2,
			columns:    []string{"c1"},
		},
		expected: nil,
	},
	"single entry, set quals only, same columns - CACHE MISS": {
		set: []cacheSetRequest{
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c1"},
				qualMap:    qualMap1,
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val"}},
				},
			},
		},
		get: cacheGetRequest{
			connection: "connection_a",
			table:      "t1",
			columns:    []string{"c1"},
		},
		expected: nil,
	},
	"single entry, get quals only, same columns - CACHE MISS": {
		set: []cacheSetRequest{
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c1"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val"}},
				},
			},
		},
		get: cacheGetRequest{
			connection: "connection_a",
			qualMap:    qualMap1,
			table:      "t1",
			columns:    []string{"c1"},
		},
		expected: nil,
	},
	"multiple entries no quals unmatched columns - CACHE MISS": {
		set: []cacheSetRequest{
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c1"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val"}},
				},
			},
			{
				connection: "connection_a",
				table:      "t1",
				columns:    []string{"c2", "c3"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c2": "c2_val"},
						{"c3": "c23val"},
					},
				},
			},
		},
		get: cacheGetRequest{
			connection: "connection_a",
			table:      "t1",
			columns:    []string{"c3", "c4"},
		},
		expected: nil,
	},
}

func TestCache(t *testing.T) {
	for name, test := range testCasesCacheTest {
		queryCache, _ := NewQueryCache()
		for _, s := range test.set {
			connectionPlugin := &steampipeconfig.ConnectionPlugin{ConnectionName: s.connection, Schema: &proto.Schema{}}
			queryCache.Set(connectionPlugin, s.table, s.qualMap, s.columns, s.result, 500*time.Second)
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)

		g := test.get
		connectionPlugin := &steampipeconfig.ConnectionPlugin{ConnectionName: g.connection, Schema: &proto.Schema{}}
		result := queryCache.Get(connectionPlugin, g.table, g.qualMap, g.columns)
		if !reflect.DeepEqual(test.expected, result) {
			t.Errorf("Test: '%s'' FAILED : \nexpected:\n %v, \n\ngot:\n %v", name, test.expected, result)
		}
	}
}
