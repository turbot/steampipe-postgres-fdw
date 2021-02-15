package cache

import (
	"reflect"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

type cacheSetRequest struct {
	table   string
	qualMap map[string]*proto.Quals
	columns []string
	result  *QueryResult
}
type cacheGetRequest struct {
	table   string
	qualMap map[string]*proto.Quals
	columns []string
	result  *QueryResult
}

type CacheTest struct {
	set      []cacheSetRequest
	get      cacheGetRequest
	expected *QueryResult
}

var testCasesCacheTest = map[string]CacheTest{

	"single entry no quals same columns": {
		set: []cacheSetRequest{
			{
				table:   "t1",
				columns: []string{"c1"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val"}},
				},
			},
		},
		get: cacheGetRequest{table: "t1",
			columns: []string{"c1"}},
		expected: &QueryResult{
			[]map[string]interface{}{
				{"c1": "c1_val"},
			},
		},
	},
	"multiple entries no quals same columns": {
		set: []cacheSetRequest{
			{
				table:   "t1",
				columns: []string{"c1"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val"}},
				},
			},
			{
				table:   "t1",
				columns: []string{"c2", "c3"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c2": "c2_val"},
						{"c3": "c23val"},
					},
				},
			},
		},
		get: cacheGetRequest{table: "t1",
			columns: []string{"c1"}},
		expected: &QueryResult{
			[]map[string]interface{}{
				{"c1": "c1_val"}},
		},
	},
	"multiple entries no quals subset of columns": {
		set: []cacheSetRequest{
			{
				table:   "t1",
				columns: []string{"c1"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c1": "c1_val"}},
				},
			},
			{
				table:   "t1",
				columns: []string{"c2", "c3"},
				result: &QueryResult{
					[]map[string]interface{}{
						{"c2": "c2_val"},
						{"c3": "c23val"},
					},
				},
			},
		},
		get: cacheGetRequest{table: "t1",
			columns: []string{"c2"}},
		expected: &QueryResult{
			[]map[string]interface{}{
				{"c2": "c2_val"},
				{"c3": "c23val"},
			},
		},
	},
}

func TestCache(t *testing.T) {
	for name, test := range testCasesCacheTest {
		queryCache := NewQueryCache()
		for _, s := range test.set {
			queryCache.Set(s.table, s.qualMap, s.columns, s.result)
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)

		g := test.get
		result := queryCache.Get(g.table, g.qualMap, g.columns)
		if !reflect.DeepEqual(test.expected, result) {
			t.Errorf("Test: '%s'' FAILED : \nexpected:\n %v, \n\ngot:\n %v", name, test.expected, result)
		}
	}
}
