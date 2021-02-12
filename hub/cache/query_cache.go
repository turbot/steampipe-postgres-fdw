package cache

import (
	"fmt"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/connection"
	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe/utils"
)

type QueryCache struct {
	resultCache *connection.Cache
	indexCache  *connection.Cache
}

func NewQueryCache() *QueryCache {
	return &QueryCache{
		resultCache: connection.NewCache(nil),
		indexCache:  connection.NewCache(nil),
	}
}

func (c QueryCache) Set(table string, qualMap map[string]*proto.Quals, columns []string, result *QueryResult) {
	// write to the result cache
	resultKey := c.BuildResultKey(table, qualMap, columns)
	c.resultCache.Set(resultKey, result)

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
	c.indexCache.Set(indexBucketKey, indexBucket)
}

func (c QueryCache) Get(table string, qualMap map[string]*proto.Quals, columns []string) []map[string]interface{} {
	// get the index bucket for this table and quals
	//- this contains cache keys for all cache entries for specified table and quals

	// get the index bucket for this table and quals
	indexBucketKey := c.BuildIndexKey(table, qualMap)
	indexBucket, ok := c.getIndex(indexBucketKey)
	if !ok {
		return nil
	}

	// now check whether we have a cache entry that covers the required columns
	indexItem := indexBucket.Get(columns)
	if indexItem == nil {
		return nil
	}

	// so we have a cache index, retrieve the item
	result, ok := c.getResult(indexItem.Key)
	if !ok {
		return nil
	}

	return result.Rows
}

// GetIndex :: retrieve an index bucket for a given cache key
func (c QueryCache) getIndex(indexKey string) (*IndexBucket, bool) {
	result, ok := c.indexCache.Get(indexKey)
	if !ok {
		return nil, false
	}
	return result.(*IndexBucket), true
}

// GetResult :: retrieve a query result for a given cache key
func (c QueryCache) getResult(resultKey string) (*QueryResult, bool) {
	result, ok := c.resultCache.Get(resultKey)
	if !ok {
		return nil, false
	}
	return result.(*QueryResult), true
}

func (c QueryCache) BuildIndexKey(table string, qualMap map[string]*proto.Quals) string {
	hash := utils.StringHash(fmt.Sprintf("%s-%s", table, grpc.QualMapToString(qualMap)))
	return fmt.Sprintf("%x", hash)
}

func (c QueryCache) BuildResultKey(table string, qualMap map[string]*proto.Quals, columns []string) string {
	hash := utils.StringHash(fmt.Sprintf("%s-%s-s", table, grpc.QualMapToString(qualMap), strings.Join(columns, ",")))
	return fmt.Sprintf("%x", hash)
}
