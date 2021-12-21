package types

import (
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/plugin"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

type keyColumnsToPathKeysTest struct {
	keyColumns []*proto.KeyColumn
	allColumns []string
	expected   []PathKey
}

var testCasesKeyColumnsToPathKeys = map[string]keyColumnsToPathKeysTest{
	"single required": {
		allColumns: []string{"id", "c1", "c2"},
		keyColumns: []*proto.KeyColumn{
			{
				Name:      "id",
				Operators: []string{"="},
				Require:   plugin.Required,
			},
		},
		expected: []PathKey{
			{
				ColumnNames: []string{"id"},
				Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "id"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "id"},
				Rows:        requiredKeyColumnBaseCost,
			},
		},
	},
	"multiple required": {
		allColumns: []string{"id", "req1", "req2", "c1", "c2"},
		keyColumns: []*proto.KeyColumn{
			{
				Name:      "id",
				Operators: []string{"="},
				Require:   plugin.Required,
			},
			{
				Name:      "req1",
				Operators: []string{"="},
				Require:   plugin.Required,
			},
			{
				Name:      "req2",
				Operators: []string{"="},
				Require:   plugin.Required,
			},
		},
		expected: []PathKey{
			{
				ColumnNames: []string{"id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
		},
	},
	"single optional": {
		allColumns: []string{"id", "c1", "c2"},
		keyColumns: []*proto.KeyColumn{
			{
				Name:      "id",
				Operators: []string{"="},
				Require:   plugin.Optional,
			},
		},
		expected: []PathKey{
			{
				ColumnNames: []string{"id"},
				Rows:        optionalKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "id"},
				Rows:        optionalKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "id"},
				Rows:        optionalKeyColumnBaseCost,
			},
		},
	},
	"multiple optional": {
		allColumns: []string{"id", "opt1", "opt2", "c1", "c2"},
		keyColumns: []*proto.KeyColumn{
			{
				Name:      "id",
				Operators: []string{"="},
				Require:   plugin.Optional,
			},
			{
				Name:      "opt1",
				Operators: []string{"="},
				Require:   plugin.Optional,
			},
			{
				Name:      "opt2",
				Operators: []string{"="},
				Require:   plugin.Optional,
			},
		},
		expected: []PathKey{
			{
				ColumnNames: []string{"id"},
				Rows:        optionalKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "id"},
				Rows:        optionalKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "id"},
				Rows:        optionalKeyColumnBaseCost,
			}, {
				ColumnNames: []string{"opt1"},
				Rows:        optionalKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "opt1"},
				Rows:        optionalKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "opt1"},
				Rows:        optionalKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"opt2"},
				Rows:        optionalKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "opt2"},
				Rows:        optionalKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "opt2"},
				Rows:        optionalKeyColumnBaseCost,
			},
		},
	},
	"required and optional": {
		allColumns: []string{"id", "opt", "c1", "c2"},
		keyColumns: []*proto.KeyColumn{
			{
				Name:      "id",
				Operators: []string{"="},
				Require:   plugin.Required,
			},
			{
				Name:      "opt",
				Operators: []string{"="},
				Require:   plugin.Optional,
			},
		},
		expected: []PathKey{
			{
				ColumnNames: []string{"id"},
				Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "id"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "id"},
				Rows:        requiredKeyColumnBaseCost,
			}, {
				ColumnNames: []string{"opt", "id"},
				Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "opt", "id"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "opt", "id"},
				Rows:        requiredKeyColumnBaseCost,
			},
		},
	},
	"multiple required and single optional": {
		allColumns: []string{"id", "req1", "req2", "opt", "c1", "c2"},
		keyColumns: []*proto.KeyColumn{
			{
				Name:      "id",
				Operators: []string{"="},
				Require:   plugin.Required,
			},
			{
				Name:      "req1",
				Operators: []string{"="},
				Require:   plugin.Required,
			},
			{
				Name:      "req2",
				Operators: []string{"="},
				Require:   plugin.Required,
			},
			{
				Name:      "opt",
				Operators: []string{"="},
				Require:   plugin.Optional,
			},
		},
		expected: []PathKey{
			{
				ColumnNames: []string{"id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"opt", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "opt", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "opt", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
		},
	},
	"multiple required and multiple optional": {
		allColumns: []string{"id", "req1", "req2", "opt1", "opt2", "opt3", "c1", "c2"},
		keyColumns: []*proto.KeyColumn{
			{
				Name:      "id",
				Operators: []string{"="},
				Require:   plugin.Required,
			},
			{
				Name:      "req1",
				Operators: []string{"="},
				Require:   plugin.Required,
			},
			{
				Name:      "req2",
				Operators: []string{"="},
				Require:   plugin.Required,
			},
			{
				Name:      "opt1",
				Operators: []string{"="},
				Require:   plugin.Optional,
			},
			{
				Name:      "opt2",
				Operators: []string{"="},
				Require:   plugin.Optional,
			},
			{
				Name:      "opt3",
				Operators: []string{"="},
				Require:   plugin.Optional,
			},
		},
		expected: []PathKey{
			{
				ColumnNames: []string{"id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"opt1", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "opt1", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "opt1", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"opt2", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "opt2", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "opt2", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"opt3", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
			},
			{
				ColumnNames: []string{"c1", "opt3", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
			{
				ColumnNames: []string{"c2", "opt3", "id", "req1", "req2"},
				Rows:        requiredKeyColumnBaseCost,
			},
		},
	},
}

func TestKeyColumnsToPathKeysTest(t *testing.T) {
	for name, test := range testCasesKeyColumnsToPathKeys {
		result := KeyColumnsToPathKeys(test.keyColumns, test.allColumns)
		if !pathKeyArraysEqual(result, test.expected) {
			t.Errorf("Test: '%s'' FAILED : expected \n%v\ngot \n%v", name, test.expected, result)
		}
	}
}

func pathKeyArraysEqual(l []PathKey, r []PathKey) bool {
	if len(l) != len(r) {
		return false
	}
	// check in both directions - inefficient but it's only a test
	for i, lkey := range l {
		eq := lkey.Equals(r[i])
		if !eq {
			return false
		}
		// does the 'r' array contain this
		//if !containsPathKey(r, lkey) {
		//	return false
		//}
	}
	//for _, rkey := range r {
	//	// does the 'r' array contain this
	//	if !containsPathKey(l, rkey) {
	//		return false
	//	}
	//}
	return true
}

func containsPathKey(keys []PathKey, otherKey PathKey) bool {
	for _, key := range keys {
		if key.Equals(otherKey) {
			return true
		}
	}
	return false
}
