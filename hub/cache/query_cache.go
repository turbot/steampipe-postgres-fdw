package cache

import (
	"fmt"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/connection"
	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
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

func (c QueryCache) Get(table string, qualMap map[string]*proto.Quals, columns []string) *QueryResult {
	fmt.Println("Get")

	// get the index bucket for this table and quals
	//- this contains cache keys for all cache entries for specified table and quals

	// get the index bucket for this table and quals
	indexBucketKey := c.BuildIndexKey(table, qualMap)
	indexBucket, ok := c.getIndex(indexBucketKey)
	if !ok {
		fmt.Printf("Get not got it %s\n", indexBucketKey)
		return nil
	}

	// now check whether we have a cache entry that covers the required columns
	indexItem := indexBucket.Get(columns)
	if indexItem == nil {
		fmt.Printf("No index item\n")
		return nil
	}

	// so we have a cache index, retrieve the item
	result, ok := c.getResult(indexItem.Key)
	if !ok {
		fmt.Printf("No result\n")
		return nil
	}

	return result
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
	str := fmt.Sprintf("%s%s", table, grpc.QualMapToString(qualMap))
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\t", "", -1)
	return str
}

func (c QueryCache) BuildResultKey(table string, qualMap map[string]*proto.Quals, columns []string) string {
	return c.BuildIndexKey(table, qualMap) + strings.Join(columns, ",")
}
