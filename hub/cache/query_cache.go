package cache

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/turbot/go-kit/types"

	"github.com/dgraph-io/ristretto"

	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

type QueryCache struct {
	cache *ristretto.Cache
	ttl   time.Duration
}

func NewQueryCache() (*QueryCache, error) {
	cache := &QueryCache{ttl: defaultTTL}

	// see if ttl env var is set
	if ttlString, ok := os.LookupEnv(CacheTTLEnvVar); ok {
		if ttlSecs, err := types.ToInt64(ttlString); err == nil {
			// weird duration syntax
			cache.ttl = time.Duration(ttlSecs) * time.Second
		}
	}

	config := &ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	}
	var err error

	if cache.cache, err = ristretto.NewCache(config); err != nil {
		return nil, err
	}
	log.Printf("[INFO] query cache created with ttl %s", cache.ttl.String())
	return cache, nil
}

func (c QueryCache) Set(table string, qualMap map[string]*proto.Quals, columns []string, result *QueryResult) {
	// write to the result cache
	resultKey := c.BuildResultKey(table, qualMap, columns)
	c.cache.SetWithTTL(resultKey, result, 1, c.ttl)

	// now update the index
	// get the index bucket for this table and quals
	indexBucketKey := c.BuildIndexKey(table, qualMap)
	indexBucket, ok := c.getIndex(indexBucketKey)

	if ok {
		indexBucket.Append(&IndexItem{columns, resultKey})
	} else {
		// create new index bucket
		indexBucket = newIndexBucket(columns, resultKey)
	}
	c.cache.SetWithTTL(indexBucketKey, indexBucket, 1, c.ttl)
}

// simple cache implemented using ristretto cache library
type Cache struct {
	cache *ristretto.Cache
}

func (cache *Cache) Set(key string, value interface{}) {
	ttl := 1 * time.Hour
	cache.cache.SetWithTTL(key, value, 1, ttl)
}

func (cache *Cache) Get(key string) (interface{}, bool) {
	return cache.cache.Get(key)
}

func (c QueryCache) Get(table string, qualMap map[string]*proto.Quals, columns []string) *QueryResult {

	// get the index bucket for this table and quals
	//- this contains cache keys for all cache entries for specified table and quals

	// get the index bucket for this table and quals
	indexBucketKey := c.BuildIndexKey(table, qualMap)
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

func (c QueryCache) BuildIndexKey(table string, qualMap map[string]*proto.Quals) string {
	str := fmt.Sprintf("index__%s%s", table, grpc.QualMapToString(qualMap))
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\t", "", -1)
	return str
}

func (c QueryCache) BuildResultKey(table string, qualMap map[string]*proto.Quals, columns []string) string {
	return c.BuildIndexKey(table, qualMap) + strings.Join(columns, ",")
}
