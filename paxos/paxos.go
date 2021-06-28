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
	votes             map[string]*common.Counter
	votesignal        map[string]chan bool
	counter           map[string]*common.Counter
	acceptsignal        map[string]chan bool
	locks             map[string]*sync.Mutex

	majority          int
	//localLog          []common.Message
}

func create_paxos_message(id string, msg_type string, message_val string, clientid string, zone string) string {
	s :=  msg_type + "/" + id + "/" + clientid + "/" + zone + "/" + message_val 
	return s
}

func NewPaxosState() *PaxosState {
	newState := PaxosState{
		counter:           make(map[string]*common.Counter),
		votes:  make(map[string]*common.Counter),
		votesignal: make(map[string]chan bool, 10),
		acceptsignal: make(map[string] chan bool),
		locks:             make(map[string]*sync.Mutex),
		majority:          0,
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}

func (state *PaxosState) majorityAchieved(count int) bool {
	return count >= state.majority  // subtract one because you can already count that you voted for yourself
}

func (state *PaxosState) SetMajority(m int) {
	state.majority = m
	fmt.Println("New majority is", m)
}

func (state *PaxosState) Initialize(clientid string ) {
	newCounter := common.Counter{
		Seq: -1,
		Count: 0,
	}

	newVoteCount := common.Counter {
		Seq: -1,
		Count: 0,
	}

	state.counter[clientid] = &newCounter
	state.votesignal[clientid] = make(chan bool)
	state.acceptsignal[clientid] = make(chan bool)
	state.votes[clientid] = &newVoteCount
	state.locks[clientid + "ACCEPTACK"] = &sync.Mutex{}
}

func (state *PaxosState) RunLeaderElection(
		id string, 
		seq_num int,
		message_val string,
		clientid string, 
		zone string, 
		broadcast func(string), 
		localbroadcast func(string),
		endorsement_signals map[string]chan string,
		e_state *endorsement.EndorsementState) {
	// Broadcast vote to everyone

	// BUG: seq num
	electionmsg := create_paxos_message(id, "LEADERELECTION_VOTE", message_val, clientid, zone)
	signatures := e_state.Run( electionmsg, seq_num, id, clientid, endorsement_signals[clientid], localbroadcast )
	state.votes[clientid].Count = 1
	state.votes[clientid].Seq = seq_num
	broadcast( "PAXOS|" + electionmsg + "/" + signatures  )
	// Wait for everyone to vote for you
	<-state.votesignal[clientid]

	return
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
	run_leader_election bool,
) bool {

	// fmt.Println("Need endorsement first")
	seq_num, _ := strconv.Atoi( strings.Split(message, "!")[1] )
	seq_num = 3 * seq_num
	if run_leader_election {
		state.RunLeaderElection(id, seq_num, message, clientid, zone, broadcast, localbroadcast, endorsement_signals, e_state)
	}

	preprepare_msg := create_paxos_message(id, "ACCEPT", message, clientid, zone)
	
	signatures := e_state.Run( preprepare_msg, seq_num + 1, id, clientid, endorsement_signals[clientid], localbroadcast )
	
	if common.VERBOSE && common.VERBOSE_EXTRA {
		fmt.Println("Got endorsement for ACCEPT")
	}
	// Get endorsement for this message
	// Do not send message to yourself. Just ack it immediately
	state.counter[clientid].Count = 1
	state.counter[clientid].Seq = seq_num + 1
	
	broadcast( "PAXOS|" + preprepare_msg + "/" + signatures )
	// committed := <-ch
	committed := <-state.acceptsignal[clientid]

	if common.VERBOSE && common.VERBOSE_EXTRA {
		fmt.Printf("Finished paxos %s\n", message)
	}


	if !committed {
		fmt.Println("Paxos failed???", message)
	}

	if common.VERBOSE && common.VERBOSE_EXTRA {
		fmt.Println("Paxos succeeded", message)
	}

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

	// fmt.Println("MESSAGE VAL", message_val)
	seq_num, _ := strconv.Atoi( strings.Split(message_val, "!")[1] ) 
	seq_num = 3 * seq_num 
	// fmt.Println("MESSAGE VAL seq_num", seq_num)

	acceptack_key := clientid + "ACCEPTACK"

	// Verify the signature first before doing anything

	switch msg_type {


	case "ACCEPT":
		s := create_paxos_message(id, "ACCEPTACK", message_val, clientid, zone)
		signatures := e_state.Run(s, seq_num + 1, id, clientid, endorsement_signals[clientid], localbroadcast)
	
		go sendMessage("PAXOS|" + s + "/" + signatures, sender_id,zone)
		
	case "ACCEPTACK":
		state.locks[acceptack_key].Lock()
		seq_num += 1
		if seq_num < state.counter[clientid].Seq  {
			state.locks[acceptack_key].Unlock()
			// fmt.Printf("Bad paxos seq num for %s, %s < %s \n", message, seq_num, state.counter[clientid].Seq)
						
			return
		} else if seq_num == state.counter[clientid].Seq {
			state.counter[clientid].Count += 1
		} else {
			state.counter[clientid].Seq = seq_num
			state.counter[clientid].Count = 1
		}
		// interf, _ := state.counter.LoadOrStore(message_val, 0)
		// count := interf.(int) 
		if state.majorityAchieved(state.counter[clientid].Count) {
			if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Printf("AcceptAck quorum for %s\n", message)
			}
			state.counter[clientid].Count = -30
			state.locks[acceptack_key].Unlock()
			s := create_paxos_message(id, "PAXOS_COMMIT", message_val, clientid, zone)

			signatures := e_state.Run( s, seq_num + 1, id, clientid, endorsement_signals[clientid], localbroadcast )
			broadcast("PAXOS|" + s + "/" + signatures)
			// Run endorsement for commit message
			// new_chan := make(chan bool)
			
			// go func(ch chan bool, s string, seq_num int, id string, clientid string,endorsement_signals map[string]chan string, e_state *endorsement.EndorsementState, localbroadcast func(string), broadcast func(string)) {
			// 	signatures := e_state.Run( s, seq_num, id, clientid, endorsement_signals[clientid], localbroadcast )
			
			// 	ch<-true
			// 	broadcast("PAXOS|" + s + "/" + signatures)
				
			// } (new_chan, s, seq_num + 1, id, clientid, endorsement_signals, e_state, localbroadcast, broadcast)
			// if common.VERBOSE && common.VERBOSE_EXTRA {
			// 	fmt.Printf("AcceptAck quorum for %s\n", message)
			// }
			// <-new_chan

			// signals[clientid] <- true
			state.acceptsignal[clientid] <- true
		} else {
			// state.counter.Store(message_val, count + 1)
			state.locks[acceptack_key].Unlock()
		}
	case "PAXOS_COMMIT":

		// Take value and commit it to the log
	case "LEADERELECTION_VOTE":
		s := create_paxos_message(id, "LEADERELECTION_REPLY", message_val, clientid, zone)
		signatures := e_state.Run(s, seq_num, id, clientid, endorsement_signals[clientid], localbroadcast)
	
		go sendMessage("PAXOS|" + s + "/" + signatures, sender_id,zone)

	case "LEADERELECTION_REPLY":
		state.locks[acceptack_key].Lock()
		if seq_num < state.votes[clientid].Seq  {
			state.locks[acceptack_key].Unlock()
			// fmt.Printf("Bad paxos2 seq num for %s, %s < %s \n", message, seq_num, state.votes[clientid].Seq)
						
			return
		} else if seq_num == state.votes[clientid].Seq {
			state.votes[clientid].Count += 1
		} else {
			state.votes[clientid].Seq = seq_num
			state.votes[clientid].Count = 1
		}

		state.votes[clientid].Count += 1
		// fmt.Println("LEADER ELECTION VOTE count:", state.votes[clientid].Count)
		if state.majorityAchieved(state.votes[clientid].Count) {
			if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Println("Leader elected", message_val)
			}
			state.votes[clientid].Count = -30
			state.votesignal[clientid] <- true
		}
		state.locks[acceptack_key].Unlock()
	}

	// Share the message with the rest of the zone
	go localbroadcast("SHARE|" + message)
}
