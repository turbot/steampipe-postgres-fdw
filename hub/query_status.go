package hub

type queryStatus string

const (
	QueryStatusReady    queryStatus = "ready"
	QueryStatusStarted  queryStatus = "started"
	QueryStatusError    queryStatus = "error"
	QueryStatusComplete queryStatus = "complete"
)
