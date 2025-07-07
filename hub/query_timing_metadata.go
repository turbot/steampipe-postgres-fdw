package hub

import (
	"log"
	"sort"
	"sync"

	"github.com/turbot/steampipe/v2/pkg/query/queryresult"
)

const (
	scanMetadataBufferSize = 500
)

type queryTimingMetadata struct {
	// map of scan metadata, keyed by query id
	// we append to this every time a scan completes (either due to end of data, or Postgres terminating)
	// each array is ordered slowest to fasteyt
	scanMetadata map[int64][]queryresult.ScanMetadataRow
	// map of scan totals, keyed by query timestamp (which is a unique query identifier
	queryRowSummary map[int64]*queryresult.QueryRowSummary

	scanMetadataLock sync.RWMutex
}

func (m *queryTimingMetadata) addScanMetadata(queryTimestamp int64, scanMetadata queryresult.ScanMetadataRow) {
	metadataForQuery := m.scanMetadata[queryTimestamp]

	if len(metadataForQuery) < scanMetadataBufferSize {
		metadataForQuery = append(metadataForQuery, scanMetadata)
		m.scanMetadata[queryTimestamp] = metadataForQuery
		// sort the metadata by decreasing time
		sort.Slice(metadataForQuery, func(i, j int) bool {
			return metadataForQuery[i].DurationMs > metadataForQuery[j].DurationMs
		})
		return
	}

	// so we have the maximum number of scan metadata items - if this scan is faster than the slowest item, ignore it
	if scanMetadata.DurationMs < metadataForQuery[len(metadataForQuery)-1].DurationMs {
		return
	}

	// add the scan metadata to the list, resort and trim to the max number of items
	metadataForQuery = append(metadataForQuery, scanMetadata)
	sort.Slice(metadataForQuery, func(i, j int) bool {
		return metadataForQuery[i].DurationMs > metadataForQuery[j].DurationMs

	})
	m.scanMetadata[queryTimestamp] = metadataForQuery[:scanMetadataBufferSize]
}

func newQueryTimingMetadata() *queryTimingMetadata {
	return &queryTimingMetadata{
		scanMetadata:    make(map[int64][]queryresult.ScanMetadataRow),
		queryRowSummary: make(map[int64]*queryresult.QueryRowSummary),
	}
}

func (m *queryTimingMetadata) removeStaleScanMetadata(currentTimestamp int64) {
	log.Printf("[INFO] removeStaleScanMetadata for current query timestamp %d", currentTimestamp)

	// clear all query metadata for previous queries
	for existingTimestamp := range m.scanMetadata {
		if existingTimestamp != currentTimestamp {
			log.Printf("[INFO] REMOVING timestamp %d", existingTimestamp)
			delete(m.scanMetadata, existingTimestamp)
			delete(m.queryRowSummary, existingTimestamp)
		}
	}
}

func (m *queryTimingMetadata) clearSummary() {
	m.scanMetadataLock.Lock()
	defer m.scanMetadataLock.Unlock()
	m.queryRowSummary = make(map[int64]*queryresult.QueryRowSummary)
}

func (m *queryTimingMetadata) clearScanMetadata() {
	m.scanMetadataLock.Lock()
	defer m.scanMetadataLock.Unlock()
	m.scanMetadata = make(map[int64][]queryresult.ScanMetadataRow)
}
