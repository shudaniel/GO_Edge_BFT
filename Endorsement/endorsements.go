package endorsement

import (
	"EdgeBFT/common"
	"strings"
	"fmt"
	"sync"
)

type EndorsementState struct {
	counter           map[string]int
	failures          int
	locks             map[string]*sync.Mutex
}

func NewEndorseState(f int) *EndorsementState {
	newState := EndorsementState{
		counter:           make(map[string]int),
		locks:  make(map[string]*sync.Mutex),
		failures: f,
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}

func createEndorseMsg(msg_type string, message string, nodeid string, original_senderid string, clientid string) string {
	return "ENDORSE|"  + msg_type + ";" + nodeid +  ";" + original_senderid + ";" + clientid + ";" + message
}

func (state *EndorsementState) GetF() int {
	return state.failures
}

func (state *EndorsementState) AddLock(clientid string ) {
	state.locks[clientid + "E_PREPARE"] = &sync.Mutex{}
	state.locks[clientid + "E_PROMISE"] = &sync.Mutex{}
}


func (state *EndorsementState) Run(
	message string, 
	id string, 
	clientid string,
	ch <-chan bool,
	broadcast func(string),

) bool {


	preprepare_msg := createEndorseMsg("E_PRE_PREPARE", message, id, id, clientid)
	
	// fmt.Printf("E_PREPARE_COUNT before sending preprepares with key: %s : %v\n", message + "E_PREPARE", state.counter[message + "E_PREPARE"])
	go broadcast(preprepare_msg)
	committed := <-ch
	return committed	
}

func (state *EndorsementState) HandleMessage(
	message string,
	broadcast func(string),
	sendMessage func(string, string, string),
	zone string,
	id string,
	signals map[string]chan bool,
) {
	components := strings.Split(message, ";")
	msg_type := components[0]
	original_senderid := components[2]
	clientid := components[3]
	msg_value := components[4]

	prepare_key := clientid + "E_PREPARE"
	promise_key := clientid + "E_PROMISE"

	switch msg_type {


	case "E_PRE_PREPARE":

		state.locks[prepare_key].Lock()
		state.counter[msg_value + "E_PREPARE"]++
		if common.HasQuorum(state.counter[msg_value + "E_PREPARE"], state.failures) {
			state.counter[msg_value + "E_PREPARE"] = -30  // Ignore all future messages
			state.locks[prepare_key].Unlock()
			// Sign the original value and send back
			signed_msg := "signed"
			s := createEndorseMsg( "E_PROMISE", msg_value, id, original_senderid, clientid ) + ";" + signed_msg
			state.locks[promise_key].Lock()
			state.counter[msg_value + "E_PROMISE"]++
			state.locks[promise_key].Unlock()
			fmt.Printf("Quorum achieved for %s\n", message)
			sendMessage(s, original_senderid, zone)
		} else {
			state.locks[prepare_key].Unlock()
		}
		
		// fmt.Printf("E_PREPREPARE_COUNT with key: %s : %v\n", msg_value + "E_PREPARE", state.counter[msg_value + "E_PREPARE"])


		s := createEndorseMsg( "E_PREPARE", msg_value, id, original_senderid, clientid )
		broadcast(s)
	case "E_PREPARE":
		
		state.locks[prepare_key].Lock()
		state.counter[msg_value + "E_PREPARE"]++
		if common.HasQuorum(state.counter[msg_value + "E_PREPARE"], state.failures) {
			state.counter[msg_value + "E_PREPARE"] = -30  // Ignore all future messages
			state.locks[prepare_key].Unlock()
			// Sign the original value and send back
			s := createEndorseMsg( "E_PROMISE", msg_value, id, original_senderid, clientid )
			state.locks[promise_key].Lock()
			state.counter[msg_value + "E_PROMISE"]++
			state.locks[promise_key].Unlock()
			fmt.Printf("Quorum achieved for %s\n", message)
			sendMessage(s, original_senderid, zone)
		} else {
			state.locks[prepare_key].Unlock()
		}
	case "E_PROMISE":
		// signature := components[4] 
		state.locks[promise_key].Lock()
		state.counter[msg_value + "E_PROMISE"]++
		if common.HasQuorum(state.counter[msg_value + "E_PROMISE"], state.failures) {
			state.counter[msg_value + "E_PROMISE"] = -30  // Ignore all future messages
			state.locks[promise_key].Unlock()
			// Value has been committed
			// Signal other channel
			if ch, ok := signals[clientid]; ok {
				fmt.Printf("Quorum achieved for endorsement %s\n", message)
				ch <- true
			}
			// Endorsement achieved
		} else {
			state.locks[promise_key].Unlock()
		}

	}
}
