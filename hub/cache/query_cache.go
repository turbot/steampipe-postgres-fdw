package cache

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/ristretto"
	goVersion "github.com/hashicorp/go-version"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"github.com/turbot/steampipe/steampipeconfig"
)

type QueryCache struct {
	cache *ristretto.Cache
	Stats *CacheStats
}

type CacheStats struct {
	// keep count of hits and misses
	Hits   int
	Misses int
}

func NewQueryCache() (*QueryCache, error) {
	cache := &QueryCache{
		Stats: &CacheStats{},
	}
	log.Printf("[INFO] CREATING QUERY CACHE")
	config := &ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	}
	var err error
	if cache.cache, err = ristretto.NewCache(config); err != nil {
		return nil, err
	}
	log.Printf("[INFO] query cache created")
	return cache, nil
}

func (c *QueryCache) Set(connection *steampipeconfig.ConnectionPlugin, table string, qualMap map[string]*proto.Quals, columns []string, limit int64, result *QueryResult, ttl time.Duration) bool {
	// if any data was returned, extract the columns from the first row
	if len(result.Rows) > 0 {
		for col := range result.Rows[0] {
			columns = append(columns, col)
		}
	}
	sort.Strings(columns)
	log.Printf("[TRACE] QueryCache Set() - connectionName: %s, table: %s, columns: %s\n", connection.ConnectionName, table, columns)

	// write to the result cache
	resultKey := c.BuildResultKey(connection, table, qualMap, columns)
	c.cache.SetWithTTL(resultKey, result, 1, ttl)

	// now update the index
	// get the index bucket for this table and quals
	indexBucketKey := c.BuildIndexKey(connection, table, qualMap)
	indexBucket, ok := c.getIndex(indexBucketKey)

	log.Printf("[TRACE] QueryCache Set() index key %s, result key %s", indexBucketKey, resultKey)

	if ok {
		indexBucket.Append(&IndexItem{columns, resultKey, limit})
	} else {
		// create new index bucket
		indexBucket = newIndexBucket().Append(NewIndexItem(columns, resultKey, limit))
	}
	return c.cache.SetWithTTL(indexBucketKey, indexBucket, 1, ttl)
}

func (c *QueryCache) Get(connection *steampipeconfig.ConnectionPlugin, table string, qualMap map[string]*proto.Quals, columns []string, limit int64) *QueryResult {
	// get the index bucket for this table and quals
	// - this contains cache keys for all cache entries for specified table and quals

	// get the index bucket for this table and quals
	indexBucketKey := c.BuildIndexKey(connection, table, qualMap)
	log.Printf("[TRACE] QueryCache Get() - index bucket key: %s\n", indexBucketKey)
	indexBucket, ok := c.getIndex(indexBucketKey)
	if !ok {
		c.Stats.Misses++
		log.Printf("[INFO] CACHE MISS - no index")
		return nil
	}

	// now check whether we have a cache entry that covers the required columns
	indexItem := indexBucket.Get(columns, limit)
	if indexItem == nil {
		limitString := "NONE"
		if limit != -1 {
			limitString = fmt.Sprintf("%d", limit)
		}
		c.Stats.Misses++
		log.Printf("[INFO] CACHE MISS - no cached data covers columns %v, limit %s\n", columns, limitString)
		return nil
	}

	// so we have a cache index, retrieve the item
	result, ok := c.getResult(indexItem.Key)
	if !ok {
		c.Stats.Misses++
		log.Printf("[INFO] CACHE MISS - no item retrieved for cache key %s", indexItem.Key)
		return nil
	}

	c.Stats.Hits++
	log.Printf("[INFO] CACHE HIT")

	return result
}

func (c *QueryCache) Clear() {
	c.cache.Clear()
}

func (c *QueryCache) BuildIndexKey(connection *steampipeconfig.ConnectionPlugin, table string, qualMap map[string]*proto.Quals) string {
	str := c.sanitiseKey(fmt.Sprintf("index__%s%s%s",
		connection.ConnectionName,
		table,
		c.formatQualMapForKey(connection, table, qualMap)))
	return str
}

func (c *QueryCache) BuildResultKey(connection *steampipeconfig.ConnectionPlugin, table string, qualMap map[string]*proto.Quals, columns []string) string {
	str := c.sanitiseKey(fmt.Sprintf("%s%s%s%s",
		connection.ConnectionName,
		table,
		c.formatQualMapForKey(connection, table, qualMap),
		strings.Join(columns, ",")))
	return str
}

// GetIndex retrieves an index bucket for a given cache key
func (c *QueryCache) getIndex(indexKey string) (*IndexBucket, bool) {
	result, ok := c.cache.Get(indexKey)
	if !ok {
		return nil, false
	}
	return result.(*IndexBucket), true
}

// GetResult retrieves a query result for a given cache key
func (c *QueryCache) getResult(resultKey string) (*QueryResult, bool) {
	result, ok := c.cache.Get(resultKey)
	if !ok {
		return nil, false
	}
	return result.(*QueryResult), true
}

func (c *QueryCache) formatQualMapForKey(connection *steampipeconfig.ConnectionPlugin, table string, qualMap map[string]*proto.Quals) string {
	var strs = make([]string, len(qualMap))
	// first build list of keys, then sort them
	keys := make([]string, len(qualMap))
	idx := 0
	for key := range qualMap {
		keys[idx] = key
		idx++
	}
	sort.Strings(keys)

	// now construct cache key from ordered quals

	// get a predicate function which tells us whether to include a qual
	shouldIncludeQualInKey := c.getShouldIncludeQualInKey(connection, table)

	for i, key := range keys {
		strs[i] = c.formatQualsForKey(qualMap[key], shouldIncludeQualInKey)
	}
	return strings.Join(strs, "-")
}

func (c *QueryCache) formatQualsForKey(quals *proto.Quals, shouldIncludeQualInKey func(string) bool) string {
	var strs []string
	for _, q := range quals.Quals {
		if shouldIncludeQualInKey(q.FieldName) {
			strs = append(strs, fmt.Sprintf("%s-%s-%v", q.FieldName, q.GetStringValue(), grpc.GetQualValue(q.Value)))
		}
	}
	return strings.Join(strs, "-")
}

// for sdk version 0.3.0 and greater, only include key column quals and optional quals
func (c *QueryCache) getShouldIncludeQualInKey(connection *steampipeconfig.ConnectionPlugin, table string) func(string) bool {
	v, err := goVersion.NewVersion(connection.Schema.SdkVersion)

	minVersionForNewCachingCode, _ := goVersion.NewVersion("0.3.0")
	if err != nil || v.LessThan(minVersionForNewCachingCode) {
		log.Printf("[TRACE] getShouldIncludeQualInKey - sdk version < 0.3.0 - using all quals for cache key")
		// for older, or unidentified sdk versions, include all quals
		return func(string) bool { return true }
	}

	log.Printf("[TRACE] getShouldIncludeQualInKey - sdk version >= 0.3.0 - only using key columns for cache key")

	// build a list of all key columns
	tableSchema, ok := connection.Schema.Schema[table]
	if !ok {
		// any errors, just default to including the column
		return func(string) bool { return true }
	}
	var cols []string
	for _, k := range tableSchema.ListCallKeyColumnList {
		cols = append(cols, k.Name)
	}
	for _, k := range tableSchema.GetCallKeyColumnList {
		cols = append(cols, k.Name)
	}

	return func(column string) bool {
		res := helpers.StringSliceContains(cols, column)
		log.Printf("[TRACE] shouldIncludeQual, column %s, include = %v", column, res)
		return res
	}

}

func (c *QueryCache) sanitiseKey(str string) string {
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\t", "", -1)
	return str
}
