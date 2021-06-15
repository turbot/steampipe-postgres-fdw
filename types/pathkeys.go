package types

import (
	"sort"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

type PathKey struct {
	ColumnNames []string
	Rows        Cost
}

func (p *PathKey) Equals(other PathKey) bool {
	sort.Strings(p.ColumnNames)
	sort.Strings(other.ColumnNames)
	return strings.Join(p.ColumnNames, ",") == strings.Join(other.ColumnNames, ",")
}

func MergePathKeys(l []PathKey, r []PathKey) []PathKey {
	res := append([]PathKey{}, l...)

	for _, p := range r {
		if !PathExistsInKeys(res, p) {
			res = append(res, p)
		}
	}
	return res
}

func PathExistsInKeys(pathKeys []PathKey, other PathKey) bool {
	for _, p := range pathKeys {
		if p.Equals(other) {
			return true
		}
	}
	return false
}

func KeyColumnsToPathKeys(required *proto.KeyColumnsSet, optional *proto.KeyColumnsSet, allColumns []string) []PathKey {
	requiredColumnSets := keyColumnsToColumnSet(required)
	optionalColumnSets := keyColumnsToColumnSet(optional)

	if len(requiredColumnSets)+len(optionalColumnSets) == 0 {
		return nil
	}

	if len(requiredColumnSets) == 0 {
		return singleKeyColumnsToPathKeys(optionalColumnSets, allColumns)
	}
	if len(optionalColumnSets) == 0 {
		return singleKeyColumnsToPathKeys(requiredColumnSets, allColumns)
	}

	return requiredAndOptionalColumnsToPathKeys(requiredColumnSets, optionalColumnSets)
}

// return a list of all the column sets to use in path keys
func keyColumnsToColumnSet(k *proto.KeyColumnsSet) [][]string {
	var res [][]string
	if k == nil {
		return res
	}

	// if a single key column is specified add it
	if k.Single != "" {
		res = append(res, []string{k.Single})
	}
	// if 'Any' key columns are specified, add them all separately
	for _, c := range k.Any {
		res = append(res, []string{c})
	}
	// if 'All' key columns are specified, add them as a single path
	if k.All != nil {
		res = append(res, k.All)
	}
	return res
}

func singleKeyColumnsToPathKeys(columnSet [][]string, allColumns []string) []PathKey {
	var res []PathKey

	// generate path keys for all permutations of required and optional
	for _, r := range columnSet {
		res = append(res, PathKey{
			ColumnNames: r,
			// make this cheap so the planner prefers to give us the qual
			Rows: 1,
		})
		//for _, c := range allColumns {
		//	res = append(res, PathKey{
		//		ColumnNames: append(r, c),
		//		// make this less cheap
		//		Rows: 10,
		//	})
		//}
	}
	return res
}

func requiredAndOptionalColumnsToPathKeys(requiredColumnSets [][]string, optionalColumnSets [][]string) []PathKey {
	var res []PathKey
	// generate path keys for all permutations of required and optional
	for _, r := range requiredColumnSets {
		// add every permutation of a single optional with the required - make this cheapest
		for _, o := range optionalColumnSets {
			columnNames := append(r, o...)
			res = append(res, PathKey{
				ColumnNames: columnNames,
				Rows:        1,
			})
		}
		// TODO do we need all other permutations??? with multiple optionals?

		// add just required - make this more expensive so the optional columns are included by preference
		res = append(res, PathKey{
			ColumnNames: r,
			Rows:        100,
		})
	}
	return res
}
