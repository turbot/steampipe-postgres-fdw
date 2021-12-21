package types

import (
	"log"
	"sort"
	"strings"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin"
)

const requiredKeyColumnBaseCost = 1
const optionalKeyColumnBaseCost = 100
const keyColumnOnlyCostMultiplier = 2

type PathKey struct {
	ColumnNames []string
	Rows        Cost
}

func (p *PathKey) Equals(other PathKey) bool {
	sort.Strings(p.ColumnNames)
	sort.Strings(other.ColumnNames)
	return strings.Join(p.ColumnNames, ",") == strings.Join(other.ColumnNames, ",") &&
		p.Rows == other.Rows
}

func KeyColumnsToPathKeys(keyColumns []*proto.KeyColumn, allColumns []string) []PathKey {
	// get the possible paths and cost for the key columns
	columnPaths, baseCost := keyColumnsToColumnPath(keyColumns)
	// remove key columns from allColumns
	allColumnsExceptKeyColumns := removeKeyColumnsFromAllColumns(keyColumns, allColumns)
	// now convert the paths to PathKeys
	return columnPathsToPathKeys(columnPaths, allColumnsExceptKeyColumns, baseCost)
}

func removeKeyColumnsFromAllColumns(keyColumns []*proto.KeyColumn, allColumns []string) []string {
	var allColumnsExceptKeyColumns = make([]string, len(allColumns)-len(keyColumns))
	idx := 0
	for _, c := range allColumns {
		if !keyColumnArrayContainsColumn(keyColumns, c) {
			allColumnsExceptKeyColumns[idx] = c
			idx++
		}
	}
	return allColumnsExceptKeyColumns
}

func keyColumnArrayContainsColumn(keyColumns []*proto.KeyColumn, c string) bool {
	for _, k := range keyColumns {
		if k.Name == c {
			return true
		}
	}
	return false
}

// keyColumnsToColumnPath returns a list of all the column sets to use in path keys
func keyColumnsToColumnPath(keyColumns []*proto.KeyColumn) (columnPaths [][]string, baseCost Cost) {
	if len(keyColumns) == 0 {
		return
	}

	// collect required and optional columns - we build a single path for all of them
	var requiredKeys, optionalKeys []string

	for _, c := range keyColumns {
		if c.Require == plugin.Required {
			requiredKeys = append(requiredKeys, c.Name)
		} else if c.Require == plugin.Optional {
			optionalKeys = append(optionalKeys, c.Name)
		} else if c.Require == plugin.AnyOf {
			// if 'Any' key columns are specified
			// build a path with just this column
			columnPaths = append(columnPaths, []string{c.Name})
		}
	}

	if len(requiredKeys) > 0 {
		// we have required keys so make the base cost CHEAP
		baseCost = requiredKeyColumnBaseCost
		// add required keys
		columnPaths = append(columnPaths, requiredKeys)
	} else {
		baseCost = optionalKeyColumnBaseCost
	}
	// if there are optional keys, add a path with required keys and each optional key
	for _, optional := range optionalKeys {
		// NOTE: append onto optional, NOT requiredKeys - otherwise we end up reusing the underlying array
		// and mutating values in columnPaths
		columnPath := append([]string{optional}, requiredKeys...)
		columnPaths = append(columnPaths, columnPath)
	}

	return
}

func LegacyKeyColumnsToPathKeys(requiredColumns, optionalColumns *proto.KeyColumnsSet, allColumns []string) []PathKey {
	requiredColumnSets := LegacyKeyColumnsToColumnPaths(requiredColumns)
	optionalColumnSets := LegacyKeyColumnsToColumnPaths(optionalColumns)

	if len(requiredColumnSets)+len(optionalColumnSets) == 0 {
		return nil
	}

	// if there are only optional, build paths based on those
	if len(requiredColumnSets) == 0 {
		return columnPathsToPathKeys(optionalColumnSets, allColumns, 1)
	}

	// otherwise build paths based just on required columns
	return columnPathsToPathKeys(requiredColumnSets, allColumns, 1)

	// TODO consider whether we need to add  paths for required+optional+other columns as well??
}

// LegacyKeyColumnsToColumnPaths returns a list of all the column sets to use in path keys
func LegacyKeyColumnsToColumnPaths(k *proto.KeyColumnsSet) [][]string {
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
func columnPathsToPathKeys(columnPaths [][]string, allColumns []string, baseCost Cost) []PathKey {

	var res []PathKey

	// generate path keys each column set
	for _, s := range columnPaths {
		// create a path for just the column path
		res = append(res, PathKey{
			ColumnNames: s,
			// make this cheap so the planner prefers to give us the qual
			Rows: baseCost * keyColumnOnlyCostMultiplier,
		})
		// also create paths for the columns path WITH each other column
		for _, c := range allColumns {
			if !helpers.StringSliceContains(s, c) {
				// NOTE: create a new slice rather than appending onto s - to avoid clash between loop iterations
				columnNames := append([]string{c}, s...)

				res = append(res, PathKey{
					ColumnNames: columnNames,
					// make this even cheaper - prefer to include all quals which were provided
					Rows: baseCost,
				})
			}
		}
	}

	log.Printf("[TRACE] columnPathsToPathKeys %d column paths %d all columns, %d pathkeys", len(columnPaths), len(allColumns), len(res))

	return res
}
