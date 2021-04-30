package pbft

import (
	"EdgeBFT/common"
	"strings"
	"fmt"
	"sync"
)

type PbftState struct {
	counter           map[string]int
	failures int
	locks             map[string]*sync.Mutex
	//localLog          []common.Message
}

func create_pbft_message(id string, msg_type string, message_val string) string {
	s := "PBFT|" + msg_type + ";" + id + ";" + message_val
	return s
}

func NewPbftState(f int) *PbftState {
	newState := PbftState{
		counter:           make(map[string]int),
		locks:             make(map[string]*sync.Mutex),
		failures: f,
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}

func (state *PbftState) GetF() int {
	return state.failures
}

func (state *PbftState) AddLock(clientid string ) {
	state.locks[clientid + "PREPARE"] = &sync.Mutex{}
	state.locks[clientid + "COMMIT"] = &sync.Mutex{}
}

func (state *PbftState) Run(
	message string, 
	id string, 
	clientid string,
	ch <-chan bool,
	broadcast func(string),

) bool {

	preprepare_msg := create_pbft_message(id,"PRE_PREPARE",  message)
	go broadcast(preprepare_msg)
	committed := <- ch
	return committed
}

func (state *PbftState) HandleMessage(
	message string,
	broadcast func(string),
	id string,
	signals map[string]chan bool,
) {
	components := strings.Split(message, ";")
	msg_type := components[0]
	message_val := components[2]


	clientid := strings.Split(message_val, "!")[0]
	prepare_key := clientid + "PREPARE"
	commit_key := clientid + "COMMIT"
	switch msg_type {


	case "PRE_PREPARE":

		

		state.locks[prepare_key].Lock()
		state.counter[message_val + "PREPARE"]++
		if common.HasQuorum(state.counter[message_val + "PREPARE"], state.failures) {
			state.counter[message_val + "PREPARE"] = -30
			state.locks[prepare_key].Unlock()	
			
			s := create_pbft_message(id, "COMMIT", message_val)
			fmt.Printf("Quorum achieved for %s\n", message)
			// state.locks[commit_key].Lock()
			// state.counter[message_val + "COMMIT"]++
			// state.locks[commit_key].Unlock()
			go broadcast(s)
		} else {
			state.locks[prepare_key].Unlock()	
		}
		s := create_pbft_message(id, "PREPARE", message_val)
		go broadcast(s)
		
		

	case "PREPARE":
		
		state.locks[prepare_key].Lock()
		state.counter[message_val + "PREPARE"]++
		if common.HasQuorum(state.counter[message_val + "PREPARE"], state.failures) {
			state.counter[message_val + "PREPARE"] = -30
			state.locks[prepare_key].Unlock()	
			s := create_pbft_message(id, "COMMIT", message_val)
			fmt.Printf("Quorum achieved for %s\n", message)
			// state.locks[commit_key].Lock()
			// state.counter[message_val + "COMMIT"]++
			// state.locks[commit_key].Unlock()
			go broadcast(s)
		} else {
			state.locks[prepare_key].Unlock()	
		}
	case "COMMIT":

		state.locks[commit_key].Lock()
		state.counter[message_val + "COMMIT"]++
		if common.HasQuorum(state.counter[message_val + "COMMIT"], state.failures) {
			state.counter[message_val + "COMMIT"] = -30
			// Value has been committed
			state.locks[commit_key].Unlock()
			// Signal other channel
			if ch, ok := signals[clientid]; ok {
				fmt.Printf("Quorum achieved for pbft %s\n", message)
				ch <- true
			}
		} else {
			state.locks[commit_key].Unlock()
		}

	}
}
