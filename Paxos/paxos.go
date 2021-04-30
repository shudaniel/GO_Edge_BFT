package paxos 

import (
	"strings"
	"EdgeBFT/endorsement"
	"sync"
)

type PaxosState struct {
	counter           sync.Map
	locks             map[string]*sync.Mutex
	//localLog          []common.Message
}

func create_paxos_message(id string, msg_type string, message_val string, clientid string, zone string) string {
	s := msg_type + "," + id + "," + clientid + "," + zone + "," + message_val + ",end"
	return s
}

func NewPaxosState() *PaxosState {
	newState := PaxosState{
		counter:           sync.Map{},
		locks:             make(map[string]*sync.Mutex),
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}

func majorityAchieved(count int) bool {
	return count >= 1  // subtract one because you can already count that you voted for yourself
}

func (state *PaxosState) Initialize(clientid string ) {
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

	// fmt.Println("Need endorsement first")
	preprepare_msg := create_paxos_message(id, "ACCEPT", message, clientid, zone)
	
	if e_state.Run( preprepare_msg, id, clientid, endorsement_signals[clientid], localbroadcast ) {
		// fmt.Println("Got endorsement")
		// Get endorsement for this message
		// Do not send message to yourself. Just ack it immediately
		
		go broadcast( "PAXOS|" + preprepare_msg)
		committed := <-ch
		return committed
	}
	return false
}

func (state *PaxosState) HandleShareMessage(message string) {
	// This is a message that the leader has processed and is sharing with you

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
		
		if e_state.Run(s, id, clientid, endorsement_signals[clientid], localbroadcast) {
			go sendMessage("PAXOS|" + s, sender_id,zone)
		}
	case "ACCEPTACK":
		state.locks[acceptack_key].Lock()
		interf, _ := state.counter.LoadOrStore(message_val, 0)
		count := interf.(int) 
		if majorityAchieved(count + 1) {
			state.counter.Store(message_val, -30)
			state.locks[acceptack_key].Unlock()
			s := create_paxos_message(id, "PAXOS_COMMIT", message_val, clientid, zone)
			// Run endorsement for commit message
			go func(s string, id string, clientid string,endorsement_signals map[string]chan bool, e_state *endorsement.EndorsementState, localbroadcast func(string), broadcast func(string)) {
				if e_state.Run( s, id, clientid, endorsement_signals[clientid], localbroadcast ) {
					broadcast("PAXOS|" + s)
				}
			} (s, id, clientid, endorsement_signals, e_state, localbroadcast, broadcast)
			// fmt.Printf("Quorum achieved for %s\n", message)
			signals[clientid] <- true
		} else {
			state.counter.Store(message_val, count + 1)
			state.locks[acceptack_key].Unlock()
		}
	case "PAXOS_COMMIT":

		// Take value and commit it to the log

	}
	// Share the message with the rest of the zone
	go localbroadcast("SHARE|" + message_val)
}
