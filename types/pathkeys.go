package types

import (
	"sort"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/plugin"

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

// KeyColumnsToColumnPath returns a list of all the column sets to use in path keys
func KeyColumnsToColumnPath(keyColumns []*proto.KeyColumn) [][]string {
	var res [][]string
	if len(keyColumns) == 0 {
		return res
	}

	// collect required columns - we buil da single path for all of them
	var required []string
	// if 'Any' key columns are specified, add them all separately
	for _, c := range keyColumns {
		if c.Require == plugin.Required {
			required = append(required, c.Name)
		} else {
			// build a path with just this column
			res = append(res, []string{c.Name})
		}
	}
	// add required as a single path

	if len(required) > 0 {
		res = append(res, required)
	}

	return res
}

func KeyColumnsToPathKeys(keyColumns []*proto.KeyColumn, allColumns []string) []PathKey {
	columnPaths := KeyColumnsToColumnPath(keyColumns)

	return columnPathsToPathKeys(columnPaths, allColumns)
}

func columnPathsToPathKeys(columnPaths [][]string, allColumns []string) []PathKey {
	var res []PathKey

	// generate path keys each column set
	for _, s := range columnPaths {
		// create a path for just the column path
		res = append(res, PathKey{
			ColumnNames: s,
			// make this cheap so the planner prefers to give us the qual
			Rows: 10,
		})
		// also create paths for the columns path WITH each other column
		for _, c := range allColumns {
			if !helpers.StringSliceContains(s, c) {
				columnNames := append(s, c)

				res = append(res, PathKey{
					ColumnNames: columnNames,
					// make this even cheaper - prefer to include all quals which were provided
					Rows: 1,
				})
			}
		}
	}
	return res
}
