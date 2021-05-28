package cache

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/ristretto"

	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

type QueryCache struct {
	cache *ristretto.Cache
}

func NewQueryCache() (*QueryCache, error) {
	cache := &QueryCache{}

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

func (c QueryCache) Set(connectionName, table string, qualMap map[string]*proto.Quals, columns []string, result *QueryResult, ttl time.Duration) {
	log.Printf("[TRACE] QueryCache Set() - connectionName: %s, table: %s, columns: %s\n", connectionName, table, columns)

	// write to the result cache
	resultKey := c.BuildResultKey(connectionName, table, qualMap, columns)
	c.cache.SetWithTTL(resultKey, result, 1, ttl)

	// now update the index
	// get the index bucket for this table and quals
	indexBucketKey := c.BuildIndexKey(connectionName, table, qualMap)
	indexBucket, ok := c.getIndex(indexBucketKey)

	log.Printf("[TRACE] QueryCache Set() index key %s, result key %s", indexBucketKey, resultKey)

	if ok {
		indexBucket.Append(&IndexItem{columns, resultKey})
	} else {
		// create new index bucket
		indexBucket = newIndexBucket(columns, resultKey)
	}
	c.cache.SetWithTTL(indexBucketKey, indexBucket, 1, ttl)
}

func (c QueryCache) Get(connectionName, table string, qualMap map[string]*proto.Quals, columns []string) *QueryResult {
	// get the index bucket for this table and quals
	// - this contains cache keys for all cache entries for specified table and quals

	// get the index bucket for this table and quals
	indexBucketKey := c.BuildIndexKey(connectionName, table, qualMap)
	log.Printf("[TRACE] QueryCache Get() - index bucket key: %s\n", indexBucketKey)
	indexBucket, ok := c.getIndex(indexBucketKey)
	if !ok {
		log.Printf("[INFO] CACHE MISS - no index\n")
		return nil
	}

	// now check whether we have a cache entry that covers the required columns
	indexItem := indexBucket.Get(columns)
	if indexItem == nil {
		log.Printf("[INFO] CACHE MISS - no cached data covers columns %v\n", columns)
		return nil
	}

	// so we have a cache index, retrieve the item
	result, ok := c.getResult(indexItem.Key)
	if !ok {
		log.Printf("[INFO] CACHE MISS - no item retrieved for cache key %s\n", indexItem.Key)
		return nil
	}

	log.Printf("[INFO] CACHE HIT")

	return result
}

// GetIndex :: retrieve an index bucket for a given cache key
func (c QueryCache) getIndex(indexKey string) (*IndexBucket, bool) {
	result, ok := c.cache.Get(indexKey)
	if !ok {
		return nil, false
	}
	return result.(*IndexBucket), true
}

// GetResult :: retrieve a query result for a given cache key
func (c QueryCache) getResult(resultKey string) (*QueryResult, bool) {
	result, ok := c.cache.Get(resultKey)
	if !ok {
		return nil, false
	}
	return result.(*QueryResult), true
}

func (c QueryCache) BuildIndexKey(connectionName, table string, qualMap map[string]*proto.Quals) string {
	str := c.sanitiseKey(fmt.Sprintf("index__%s%s%s",
		connectionName,
		table,
		formatQualMapForKey(qualMap)))
	return str
}

func (c QueryCache) BuildResultKey(connectionName, table string, qualMap map[string]*proto.Quals, columns []string) string {
	str := c.sanitiseKey(fmt.Sprintf("%s%s%s%s",
		connectionName,
		table,
		formatQualMapForKey(qualMap),
		strings.Join(columns, ",")))
	return str
}

func formatQualMapForKey(qualMap map[string]*proto.Quals) string {
	var strs = make([]string, len(qualMap))
	// first build list of keys, then sort them
	keys := make([]string, len(qualMap))
	idx := 0
	for key := range qualMap {
		keys[idx] = key
		idx++
	}
	log.Printf("[TRACE] formatQualMapForKey unsorted keys %v\n", keys)
	sort.Strings(keys)
	log.Printf("[TRACE] formatQualMapForKey sorted keys %v\n", keys)

	// now construct cache key from ordered quals
	for i, key := range keys {
		strs[i] = formatQualsForKey(qualMap[key])
	}
	return strings.Join(strs, "-")
}

func formatQualsForKey(quals *proto.Quals) string {
	var strs = make([]string, len(quals.Quals))
	for i, q := range quals.Quals {
		strs[i] = fmt.Sprintf("%s-%s-%v", q.FieldName, q.GetStringValue(), grpc.GetQualValue(q.Value))
	}
	return strings.Join(strs, "-")
}

func (c QueryCache) sanitiseKey(str string) string {
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\t", "", -1)
	return str
}
