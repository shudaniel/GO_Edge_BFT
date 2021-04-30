package endorsement

import (
	"EdgeBFT/common"
	"strings"
	"fmt"
)

type EndorsementState struct {
	counter           map[string]int
	failures int
}

func NewEndorseState(f int) *EndorsementState {
	newState := EndorsementState{
		counter:           make(map[string]int),
		failures: f,
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}


func (state *EndorsementState) incrementState(key string) {
	if _, ok := state.counter[key]; ok {
    	//do something here
		state.counter[key]++
	} else {
		state.counter[key] = 1
	}
}

func createEndorseMsg(msg_type string, message string, nodeid string, original_senderid string, clientid string) string {
	return "ENDORSE|"  + msg_type + ";" + nodeid +  ";" + original_senderid + ";" + clientid + ";" + message
}

func (state *EndorsementState) GetF() int {
	return state.failures
}

func (state *EndorsementState) Run(
	message string, 
	id string, 
	clientid string,
	ch <-chan bool,
	broadcast func(string),

) bool {


	preprepare_msg := createEndorseMsg("PRE_PREPARE", message, id, id, clientid)
	// Don't send to yourself. Just ack your own message immediately
	state.counter[message + "PREPARE"] = 1
	go broadcast(preprepare_msg)
	committed := <-ch
	return committed	
}

func (state *EndorsementState) HandleMessage(
	message string,
	broadcast func(string),
	sendMessage func(string, string),
	id string,
	signals map[string]chan bool,
) {
	components := strings.Split(message, ";")
	msg_type := components[0]
	original_senderid := components[1]
	clientid := components[2]
	msg_value := components[3]
	switch msg_type {


	case "PRE_PREPARE":
		s := createEndorseMsg( "PREPARE", msg_value, id, original_senderid, clientid )
		go broadcast(s)
	case "PREPARE":
		
		state.incrementState(msg_value + "PREPARE")
		if common.HasQuorum(state.counter[msg_value], state.failures) {

			// Sign the original value and send back
			signed_msg := "signed"
			s := createEndorseMsg( "PROMISE", msg_value, id, original_senderid, clientid ) + ";" + signed_msg
			state.incrementState(msg_value + "PROMISE")
			fmt.Printf("Quorum achieved for %s\n", message)
			go sendMessage(s, original_senderid)
		}
	case "PROMISE":
		// signature := components[4] 
		state.incrementState(msg_value + "PROMISE")
		if common.HasQuorum(state.counter[msg_value], state.failures) {
			// Value has been committed
			// Signal other channel
			if ch, ok := signals[clientid]; ok {
				fmt.Printf("Quorum achieved for endorsement %s\n", message)
				ch <- true
			}
			// Endorsement achieved
		}

	}
}
