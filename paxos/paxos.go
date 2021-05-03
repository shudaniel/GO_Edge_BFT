package paxos 

import (
	"strings"
	"EdgeBFT/common"
	"EdgeBFT/endorsement"
	"sync"
	"fmt"
	"strconv"
)

type PaxosState struct {
	counter           map[string]*common.Counter
	locks             map[string]*sync.Mutex
	//localLog          []common.Message
}

func create_paxos_message(id string, msg_type string, message_val string, clientid string, zone string) string {
	s :=  msg_type + "/" + id + "/" + clientid + "/" + zone + "/" + message_val 
	return s
}

func NewPaxosState() *PaxosState {
	newState := PaxosState{
		counter:           make(map[string]*common.Counter),
		locks:             make(map[string]*sync.Mutex),
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}

func majorityAchieved(count int) bool {
	return count >= common.MAJORITY  // subtract one because you can already count that you voted for yourself
}

func (state *PaxosState) Initialize(clientid string ) {
	newCounter := common.Counter{
		Seq: -1,
		Count: 0,
	}

	state.counter[clientid] = &newCounter
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
	endorsement_signals map[string]chan string,
	e_state *endorsement.EndorsementState,
) bool {

	// fmt.Println("Need endorsement first")
	seq_num, _ := strconv.Atoi( strings.Split(message, "!")[1] )
	preprepare_msg := create_paxos_message(id, "ACCEPT", message, clientid, zone)
	
	signatures := e_state.Run( preprepare_msg, seq_num, id, clientid, endorsement_signals[clientid], localbroadcast )
	
	if common.VERBOSE && common.VERBOSE_EXTRA {
		fmt.Println("Got endorsement for ACCEPT")
	}
	// Get endorsement for this message
	// Do not send message to yourself. Just ack it immediately
	state.counter[clientid].Count = 1
	state.counter[clientid].Seq = seq_num
	
	go broadcast( "PAXOS|" + preprepare_msg + "/" + signatures )
	committed := <-ch
	if !committed {
		fmt.Println("Paxos failed???")
	}

	if common.VERBOSE && common.VERBOSE_EXTRA {
		fmt.Println("Paxos succeeded")
	}

	return committed
	
	return true
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
	endorsement_signals map[string]chan string,
	e_state *endorsement.EndorsementState,
) {
	components := strings.Split(message, "/")

	msg_type := components[0]
	sender_id := components[1]
	clientid := components[2]
	zone := components[3]
	message_val := components[4]

	seq_num, _ := strconv.Atoi( strings.Split(message_val, "!")[1] )

	acceptack_key := clientid + "ACCEPTACK"

	// Verify the signature first before doing anything

	switch msg_type {


	case "ACCEPT":
		s := create_paxos_message(id, "ACCEPTACK", message_val, clientid, zone)
		signatures := e_state.Run(s, seq_num, id, clientid, endorsement_signals[clientid], localbroadcast)
	
		go sendMessage("PAXOS|" + s + "/" + signatures, sender_id,zone)
		
	case "ACCEPTACK":
		state.locks[acceptack_key].Lock()

		if seq_num < state.counter[clientid].Seq  {
			state.locks[acceptack_key].Unlock()
			return
		} else if seq_num == state.counter[clientid].Seq {
			state.counter[clientid].Count += 1
		} else {
			state.counter[clientid].Seq = seq_num
			state.counter[clientid].Count = 1
		}
		// interf, _ := state.counter.LoadOrStore(message_val, 0)
		// count := interf.(int) 
		if majorityAchieved(state.counter[clientid].Count) {
			state.counter[clientid].Count = -30
			state.locks[acceptack_key].Unlock()
			s := create_paxos_message(id, "PAXOS_COMMIT", message_val, clientid, zone)
			// Run endorsement for commit message
			new_chan := make(chan bool)
			go func(ch chan bool, s string, seq_num int, id string, clientid string,endorsement_signals map[string]chan string, e_state *endorsement.EndorsementState, localbroadcast func(string), broadcast func(string)) {
				signatures := e_state.Run( s, seq_num, id, clientid, endorsement_signals[clientid], localbroadcast )
			
				ch<-true
				broadcast("PAXOS|" + s + "/" + signatures)
				
			} (new_chan, s, seq_num, id, clientid, endorsement_signals, e_state, localbroadcast, broadcast)
			if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Printf("AcceptAck quorum for %s\n", message)
			}
			<-new_chan
			signals[clientid] <- true
		} else {
			// state.counter.Store(message_val, count + 1)
			state.locks[acceptack_key].Unlock()
		}
	case "PAXOS_COMMIT":

		// Take value and commit it to the log

	}
	// Share the message with the rest of the zone
	go localbroadcast("SHARE|" + message_val)
}
