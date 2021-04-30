package pbft

import (
	"EdgeBFT/common"
	"strings"
	"fmt"
)

type PbftState struct {
	counter           map[string]int
	failures int
	//localLog          []common.Message
}

func create_pbft_message(id string, msg_type string, message_val string) string {
	s := "PBFT|" + msg_type + ";" + id + ";" + message_val
	return s
}

func NewPbftState(f int) *PbftState {
	newState := PbftState{
		counter:           make(map[string]int),
		failures: f,
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}

func (state *PbftState) GetF() int {
	return state.failures
}

func (state *PbftState) incrementState(key string) {
	if _, ok := state.counter[key]; ok {
    	//do something here
		state.counter[key]++
	} else {
		state.counter[key] = 1
	}
}

func (state *PbftState) Run(
	message string, 
	id string, 
	ch <-chan bool,
	broadcast func(string),

) bool {

	preprepare_msg := create_pbft_message(id, message, "PRE_PREPARE")
	state.counter[message + "PREPARE"] = 1
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
	switch msg_type {


	case "PRE_PREPARE":
		s := create_pbft_message(id, "PREPARE", message_val)
		go broadcast(s)
	case "PREPARE":
		
		state.incrementState(message_val + "PREPARE")
		if common.HasQuorum(state.counter[message_val], state.failures) {
			s := create_pbft_message(id, "COMMIT", message_val)
			fmt.Printf("Quorum achieved for %s\n", message)
			state.incrementState(message_val + "COMMIT")
			go broadcast(s)
		}
	case "COMMIT":

		state.incrementState(message_val + "COMMIT")
		if common.HasQuorum(state.counter[message_val], state.failures) {
			// Value has been committed
			// Signal other channel
			if ch, ok := signals[clientid]; ok {
				fmt.Printf("Quorum achieved for pbft %s\n", message)
				ch <- true
			}
		}

	}
}
