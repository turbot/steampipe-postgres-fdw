package types

import (
	"log"
	"sort"
	"strings"

	"github.com/turbot/go-kit/helpers"

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

	//if len(requiredColumnSets) == 0 {
	return singleKeyColumnsToPathKeys(requiredColumnSets, allColumns)
	//}
	//if len(optionalColumnSets) == 0 {
	//	return singleKeyColumnsToPathKeys(requiredColumnSets, allColumns)
	//}
	//
	//return requiredAndOptionalColumnsToPathKeys(requiredColumnSets, optionalColumnSets, allColumns)
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
			Rows: 10,
		})
		for _, c := range allColumns {
			if !helpers.StringSliceContains(r, c) {
				columnNames := append(r, c)

				res = append(res, PathKey{
					ColumnNames: columnNames,
					// make this less cheap
					Rows: 1,
				})
			}
		}
	}
	return res
}

func requiredAndOptionalColumnsToPathKeys(requiredColumnSets [][]string, optionalColumnSets [][]string, allColumns []string) []PathKey {
	var res []PathKey
	// generate path keys for all permutations of required and optional
	for _, r := range requiredColumnSets {
		// add just required - make this more expensive so the optional columns are included by preference
		res = append(res, PathKey{
			ColumnNames: r,
			Rows:        10,
		})
		for _, c := range allColumns {
			if !helpers.StringSliceContains(r, c) {
				columnNames := append(r, c)
				log.Printf("[WARN] requiredAndOptionalColumnsToPathKeys cols %v", columnNames)
				res = append(res, PathKey{
					ColumnNames: columnNames,
					// make this less cheap
					Rows: 1,
				})
			}
		}
	}
	return res
}
