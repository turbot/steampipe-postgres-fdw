package cache

import "github.com/turbot/go-kit/helpers"

// each index bucket contains index items for all cache results for a given table and qual set
type IndexBucket struct {
	Items []*IndexItem
}

func newIndexBucket(columns []string, key string) *IndexBucket {
	return &IndexBucket{
		Items: []*IndexItem{&IndexItem{
			Columns: columns,
			Key:     key,
		}},
	}
}

func (b *IndexBucket) Append(item *IndexItem) {
	b.Items = append(b.Items, item)
}

// find an index item which satisfies all columns
func (b *IndexBucket) Get(columns []string) *IndexItem {
	for _, item := range b.Items {
		if item.SatisfiesColumns(columns) {
			return item
		}
	}
	return nil
}

// each index item has the columns and cached index for a single cached query result
// note - this index item it tied to a specific table and set of quals
type IndexItem struct {
	Columns []string
	Key     string
}

func (i IndexItem) SatisfiesColumns(columns []string) bool {
	// if there are no columns specified, assume this item covers ALL columns
	if len(i.Columns) == 0 {
		return true
	}
	for _, c := range columns {
		if !helpers.StringSliceContains(i.Columns, c) {
			return false
		}

	}
	return true
}
