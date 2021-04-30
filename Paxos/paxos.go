package paxos 

import (
	"strings"
	"fmt"
	"EdgeBFT/endorsement"
	"sync"
)

type PaxosState struct {
	counter           map[string]int
	locks             map[string]*sync.Mutex
	//localLog          []common.Message
}

func create_paxos_message(id string, msg_type string, message_val string, clientid string, zone string) string {
	s := msg_type + "," + id + "," + clientid + "," + zone + "," + message_val
	return s
}

func NewPaxosState() *PaxosState {
	newState := PaxosState{
		counter:           make(map[string]int),
		locks:             make(map[string]*sync.Mutex),
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}

func majorityAchieved(count int) bool {
	return count >= 1  // subtract one because you can already count that you voted for yourself
}

func (state *PaxosState) AddLock(clientid string ) {
	state.locks[clientid + "ACCEPTACK"] = &sync.Mutex{}
}


func (state *PaxosState) Run(
	message string, 
	id string, 
	zone string,
	clientid string,
	ch <-chan bool,
	broadcast func(string),
	localbroadcast func(string),
	endorsement_signals map[string]chan bool,
	e_state *endorsement.EndorsementState,
) bool {

	fmt.Println("Need endorsement first")
	preprepare_msg := create_paxos_message(id, "ACCEPT", message, clientid, zone)
	endorsement_signals[clientid] = make(chan bool)
	if e_state.Run( preprepare_msg, id, clientid, endorsement_signals[clientid], localbroadcast ) {
		fmt.Println("Got endorsement")
		// Get endorsement for this message
		// Do not send message to yourself. Just ack it immediately
		
		go broadcast( "PAXOS|" + preprepare_msg)
		committed := <-ch
		return committed
	}
	return false
}

func (state *PaxosState) HandleMessage(
	message string,
	broadcast func(string),
	localbroadcast func(string),
	sendMessage func (string, string, string),
	id string,
	signals map[string]chan bool,
	endorsement_signals map[string]chan bool,
	e_state *endorsement.EndorsementState,
) {
	components := strings.Split(message, ",")
	msg_type := components[0]
	sender_id := components[1]
	clientid := components[2]
	zone := components[3]
	message_val := components[4]

	acceptack_key := clientid + "ACCEPTACK"

	// Verify the signature first before doing anything

	switch msg_type {


	case "ACCEPT":
		s := create_paxos_message(id, "ACCEPTACK", message_val, clientid, zone)
		endorsement_signals[clientid]  = make(chan bool)
		if e_state.Run(s, id, clientid, endorsement_signals[clientid], localbroadcast) {
			go sendMessage("PAXOS|" + s, sender_id,zone)
		}
	case "ACCEPTACK":
		state.locks[acceptack_key].Lock()
		state.counter[message_val]++
		if majorityAchieved(state.counter[message_val]) {
			state.counter[message_val] = -30
			state.locks[acceptack_key].Unlock()
			s := create_paxos_message(id, "PAXOS_COMMIT", message_val, clientid, zone)

			// Run endorsement for commit message
			fmt.Printf("Quorum achieved for %s\n", message)
			signals[clientid] <- true
			go broadcast("PAXOS|" + s)
		} else {
			state.locks[acceptack_key].Unlock()
		}
	case "PAXOS_COMMIT":

		// Take value and commit it to the log

	}
}
