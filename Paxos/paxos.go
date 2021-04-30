package paxos 

import (
	"strings"
	"fmt"
	"EdgeBFT/endorsement"
)

type PaxosState struct {
	counter           map[string]int
	//localLog          []common.Message
}

func create_paxos_message(id string, msg_type string, message_val string, clientid string) string {
	s := "PAXOS|" + msg_type + ";" + id + ";" + clientid + ";" + message_val
	return s
}

func NewPaxosState() *PaxosState {
	newState := PaxosState{
		counter:           make(map[string]int),
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}

func majorityAchieved(count int) bool {
	return count >= 2
}

func (state *PaxosState) Run(
	message string, 
	id string, 
	clientid string,
	ch <-chan bool,
	broadcast func(string),
	localbroadcast func(string),
	endorsement_signals map[string]chan bool,
	e_state *endorsement.EndorsementState,
) bool {

	preprepare_msg := create_paxos_message(id, message, "ACCEPT", clientid)
	endorsement_signals[clientid] = make(chan bool)
	if e_state.Run( preprepare_msg, id, clientid, endorsement_signals[clientid], localbroadcast ) {
		// Get endorsement for this message
		// Do not send message to yourself. Just ack it immediately
		state.counter[message] = 1
		
		go broadcast(preprepare_msg)
		committed := <-ch
		return committed
	}
	return false
}

func (state *PaxosState) HandleMessage(
	message string,
	broadcast func(string),
	localbroadcast func(string),
	id string,
	signals map[string]chan bool,
	endorsement_signals map[string]chan bool,
	e_state *endorsement.EndorsementState,
) {
	components := strings.Split(message, ";")
	msg_type := components[0]
	clientid := components[2]
	message_val := components[3]

	// Verify the signature first before doing anything

	switch msg_type {


	case "ACCEPT":
		s := create_paxos_message(id, "ACCEPTACK", message_val, clientid)
		endorsement_signals[clientid]  = make(chan bool)
		if e_state.Run(s, id, clientid, endorsement_signals[clientid], localbroadcast) {
			go broadcast(s)
		}
	case "ACCEPTACK":
		
		state.counter[message_val]++
		if majorityAchieved(state.counter[message_val]) {
			s := create_paxos_message(id, "COMMIT", message_val, clientid)
			fmt.Printf("Quorum achieved for %s\n", message)
			signals[clientid] <- true
			go broadcast(s)
		}
	case "COMMIT":

		// Take value and commit it to the log

	}
}
