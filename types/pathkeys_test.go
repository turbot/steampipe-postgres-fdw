package types

import (
	"fmt"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"golang.org/x/exp/slices"
	"testing"
)

func TestKeyColumnsToPathKeys(t *testing.T) {
	tests := []struct {
		name       string
		keyColumns []*proto.KeyColumn
		allColumns []string
		want       []PathKey
	}{
		{
			name:       "single required",
			allColumns: []string{"id", "c1", "c2"},
			keyColumns: []*proto.KeyColumn{
				{
					Name:      "id",
					Operators: []string{"="},
					Require:   plugin.Required,
				},
			},
			want: []PathKey{
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
		{
			name:       "single required and 2 any of",
			allColumns: []string{"id", "anyof1", "anyof2", "c3"},
			keyColumns: []*proto.KeyColumn{
				{
					Name:      "id",
					Operators: []string{"="},
					Require:   plugin.Required,
				},
				{
					Name:      "anyof1",
					Operators: []string{"="},
					Require:   plugin.AnyOf,
				},
				{
					Name:      "anyof2",
					Operators: []string{"="},
					Require:   plugin.AnyOf,
				},
			},
			want: []PathKey{
				{
					ColumnNames: []string{"anyof1", "id"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"anyof1", "c3", "id"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{

					ColumnNames: []string{"anyof2", "id"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"anyof2", "c3", "id"},
					Rows:        requiredKeyColumnBaseCost,
				},
			},
		},
		{
			name:       "multiple required",
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
			want: []PathKey{
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
		{
			name:       "multiple any of",
			allColumns: []string{"anyof1", "anyof2", "c1", "c2"},
			keyColumns: []*proto.KeyColumn{
				{
					Name:      "anyof1",
					Operators: []string{"="},
					Require:   plugin.AnyOf,
				},
				{
					Name:      "anyof2",
					Operators: []string{"="},
					Require:   plugin.AnyOf,
				},
			},
			want: []PathKey{
				{
					ColumnNames: []string{"anyof1"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"anyof1", "c1"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"anyof1", "c2"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"anyof2"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"anyof2", "c1"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"anyof2", "c2"},
					Rows:        requiredKeyColumnBaseCost,
				},
			},
		},
		{
			name:       "multiple any of and single optional",
			allColumns: []string{"anyof1", "anyof2", "opt", "c1", "c2"},
			keyColumns: []*proto.KeyColumn{
				{
					Name:      "anyof1",
					Operators: []string{"="},
					Require:   plugin.AnyOf,
				},
				{
					Name:      "anyof2",
					Operators: []string{"="},
					Require:   plugin.AnyOf,
				},
				{
					Name:      "opt",
					Operators: []string{"="},
					Require:   plugin.Optional,
				},
			},

			want: []PathKey{
				{
					ColumnNames: []string{"anyof1"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"anyof1", "c1"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"anyof1", "c2"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"anyof2"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"anyof2", "c1"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"anyof2", "c2"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"anyof1", "opt"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"anyof1", "c1", "opt"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"anyof1", "c2", "opt"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"anyof2", "opt"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"anyof2", "c1", "opt"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"anyof2", "c2", "opt"},
					Rows:        requiredKeyColumnBaseCost,
				},
			},
		},
		{
			name:       "multiple any of and multiple optional",
			allColumns: []string{"anyof1", "anyof2", "opt1", "opt2", "c1", "c2"},
			keyColumns: []*proto.KeyColumn{
				{
					Name:      "anyof1",
					Operators: []string{"="},
					Require:   plugin.AnyOf,
				},
				{
					Name:      "anyof2",
					Operators: []string{"="},
					Require:   plugin.AnyOf,
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
			want: []PathKey{
				{
					ColumnNames: []string{"anyof1"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"c1", "anyof1"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"c2", "anyof1"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"anyof2"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"c1", "anyof2"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"c2", "anyof2"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"opt1", "anyof1"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"c1", "opt1", "anyof1"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"c2", "opt1", "anyof1"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"opt1", "anyof2"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"c1", "opt1", "anyof2"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"c2", "opt1", "anyof2"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"opt2", "anyof1"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"c1", "opt2", "anyof1"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"c2", "opt2", "anyof1"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"opt2", "anyof2"},
					Rows:        requiredKeyColumnBaseCost * keyColumnOnlyCostMultiplier,
				},
				{
					ColumnNames: []string{"c1", "opt2", "anyof2"},
					Rows:        requiredKeyColumnBaseCost,
				},
				{
					ColumnNames: []string{"c2", "opt2", "anyof2"},
					Rows:        requiredKeyColumnBaseCost,
				},
			},
		},
		{
			name:       "single optional",
			allColumns: []string{"id", "c1", "c2"},
			keyColumns: []*proto.KeyColumn{
				{
					Name:      "id",
					Operators: []string{"="},
					Require:   plugin.Optional,
				},
			},
			want: []PathKey{
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
		{
			name:       "multiple optional",
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
			want: []PathKey{
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
		{
			name:       "required and optional",
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
			want: []PathKey{
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
		{
			name:       "multiple required and single optional",
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
			want: []PathKey{
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
		{
			name:       "multiple required and multiple optional",
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
			want: []PathKey{
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := KeyColumnsToPathKeys(tt.keyColumns, tt.allColumns)
			//for _, pk := range got {
			//	slices.Sort(pk.ColumnNames)
			//}

			for _, pk := range got {
				slices.Sort(pk.ColumnNames)
				fmt.Printf("PathKey: %v, Rows: %f\n", pk.ColumnNames, pk.Rows)
			}

			for _, pk := range tt.want {
				slices.Sort(pk.ColumnNames)
				fmt.Printf("Expected PathKey%v, Rows: %f\n", pk.ColumnNames, pk.Rows)
			}

			if !pathKeyArraysEqual(got, tt.want) {
				t.Errorf("KeyColumnsToPathKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

//	func TestKeyColumnsToPathKeysTest(t *testing.T) {
//		for name, test := range testCasesKeyColumnsToPathKeys {
//			result := KeyColumnsToPathKeys(test.keyColumns, test.allColumns)
//			// for simplicity of testing order all the PathKeys by their column names
//			for _, pk := range result {
//				slices.Sort(pk.ColumnNames)
//			}
//			if !pathKeyArraysEqual(result, test.want) {
//				t.Errorf("Test: '%s'' FAILED : want \n%v\ngot \n%v", name, test.want, result)
//			}
//		}
//	}
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
	}

	return true
}
