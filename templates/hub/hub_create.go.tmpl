package hub

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"sync"
)

// global hub instance
var hubSingleton Hub

// mutex protecting hub creation
var hubMux sync.Mutex

// GetHub returns a hub singleton
func GetHub() Hub {
	// lock access to singleton
	hubMux.Lock()
	defer hubMux.Unlock()
	return hubSingleton

}

// create the hub
func CreateHub() error {
	logging.LogTime("GetHub start")

	// lock access to singleton
	hubMux.Lock()
	defer hubMux.Unlock()

	var err error
	// TODO configure build to select between local and remote hub
	// TODO get connection config from import foreign schema options

	hubSingleton, err = newLocalHub()
	if err != nil {
		return err
	}
	logging.LogTime("GetHub end")
	return err
}
