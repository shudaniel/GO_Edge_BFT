package tracker

import (
	"encoding/json"
	"fmt"
)

type TimeTracker struct {
	ReceivedNumTxns int `json:"numtxn"`
	TotalLatencies float64`json:"totallatency"`

}

type ClientsTracker struct {
	tracker map[string]*TimeTracker 
}

func NewClientsTracker() *ClientsTracker {
	NewClientsTracker := ClientsTracker{
		tracker: make(map[string]*TimeTracker),
	}

	return &NewClientsTracker
}

func NewTimeTracker() *TimeTracker {

	NewTimeTracker := TimeTracker {
		ReceivedNumTxns: 0,
		TotalLatencies: 0,
	}

	return &NewTimeTracker
}

func (t *ClientsTracker) AddClient(clientid string) {
	t.tracker[clientid] = NewTimeTracker()
}

func (t *ClientsTracker) AddLatency(clientid string, latency float64) {
	t.tracker[clientid].TotalLatencies += latency
	t.tracker[clientid].ReceivedNumTxns++
}

func (t *ClientsTracker) GenerateReturnData() string {
	
	// Fix pointer issue

	jsonData, err := json.Marshal(t.tracker)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("JSON:", string(jsonData))
	return string(jsonData)
}
