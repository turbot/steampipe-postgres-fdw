package types

import (
	"log"
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

func KeyColumnsToPathKeys(k *proto.KeyColumnsSet) []PathKey {
	log.Printf("[WARN] keyColumnsToPathKeys %v\n", k)
	var res []PathKey
	// if a single key column is specified add it
	if k.Single != "" {
		res = append(res, PathKey{
			ColumnNames: []string{k.Single},
			Rows:        1,
		})
	}
	// if 'Any' key columns are specified, add them all separately
	for _, c := range k.Any {
		res = append(res, PathKey{
			ColumnNames: []string{c},
			Rows:        1,
		})
	}
	// if 'All' key columns are specified, add them as a single path
	if k.All != nil {
		res = append(res, PathKey{
			ColumnNames: k.All,
			Rows:        1,
		})
	}
	return res
}
