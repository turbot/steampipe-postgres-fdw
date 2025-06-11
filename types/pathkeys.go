package types

import (
	"log"
	"slices"
	"sort"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
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
func keyColumnsToColumnPath(keyColumns []*proto.KeyColumn) ([][]string, Cost) {
	var columnPaths [][]string
	var baseCost Cost

	if len(keyColumns) == 0 {
		return columnPaths, baseCost
	}

	// collect required and optional and any of keys
	var requiredKeys, optionalKeys, anyOfKeys []string
	for _, c := range keyColumns {
		if c.Require == plugin.Required {
			requiredKeys = append(requiredKeys, c.Name)
		} else if c.Require == plugin.Optional {
			optionalKeys = append(optionalKeys, c.Name)
		} else if c.Require == plugin.AnyOf {
			anyOfKeys = append(anyOfKeys, c.Name)
		}
	}

	var requiredColumnPaths [][]string
	// if we have any-of keys, for each any-of key we will create a separate path containing that key and all required keys
	if len(anyOfKeys) > 0 {
		for _, a := range anyOfKeys {
			columnPath := append([]string{a}, requiredKeys...)
			requiredColumnPaths = append(requiredColumnPaths, columnPath)
		}
	} else if len(requiredKeys) > 0 {
		// add required keys as a single path
		requiredColumnPaths = append(requiredColumnPaths, requiredKeys)
	}

	// if we have any column paths (meaning we have aither required or any-of columns),
	// we have required keys so make the base cost CHEAP
	if len(requiredColumnPaths) > 0 {
		baseCost = requiredKeyColumnBaseCost
	} else {
		baseCost = optionalKeyColumnBaseCost
	}

	// create output column paths - init to the required column paths - we will add permutations with optional keys
	columnPaths = requiredColumnPaths
	// now create additional paths based on the required paths, with each of the optional keys
	for _, optional := range optionalKeys {
		col := optional
		// NOTE: append onto optional, NOT requiredKeys - otherwise we end up reusing the underlying array
		// and mutating values in columnPaths

		// if there are any required keys, build path from optional AND required
		if len(requiredColumnPaths) > 0 {
			for _, requiredPath := range requiredColumnPaths {
				p := append(append([]string{}, requiredPath...), col)
				columnPaths = append(columnPaths, p)
			}
		} else {
			// if there are no required keys or anyof keys, just create a path from the optional
			columnPaths = append(columnPaths, []string{col})
		}
	}

	return columnPaths, baseCost
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
			if !slices.Contains(s, c) {
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
