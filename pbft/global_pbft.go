package pbft

import (
	"EdgeBFT/common"
	"strings"
	"sync"
	"fmt"
	"strconv"
)

// Global version of PBFT

type Counter struct {
	Count map[string]int
	Seq int
}

type PbftGlobalState struct {
	counter_prepare map[string]*Counter
	counter_commit map[string]*Counter
	// counter_prepare           sync.Map
	// counter_commit           sync.Map
	failures int
	locks             map[string]*sync.Mutex
	//localLog          []common.Message
}

func create_global_pbft_message(clientid string, id string, zone string, msg_type string, message_val string) string {
	s := "PBFT_GLOBAL|" + clientid + "|" + zone + "|" + msg_type + ";" + id + ";" + message_val 
	return s
}

func NewPbftGlobalState(f int) *PbftGlobalState {
	newState := PbftGlobalState{
		counter_prepare: make(map[string]*Counter),
		counter_commit: make(map[string]*Counter),

		locks:             make(map[string]*sync.Mutex),
		failures: f,
	}

	return &newState
}

func (state *PbftGlobalState) GetF() int {
	return state.failures
}


func (state *PbftGlobalState) Initialize(clientid string ) {
	newPrepareCounter := Counter {
		Seq: -1,
		Count: make(map[string]int),
	}

	newPrepareCounter.Count["0"] = 0
	newPrepareCounter.Count["1"] = 0
	newPrepareCounter.Count["2"] = 0

	newCommitCounter := Counter {
		Seq: -1,
		Count: make(map[string]int),
	}

	newCommitCounter.Count["0"] = 0
	newCommitCounter.Count["1"] = 0
	newCommitCounter.Count["2"] = 0


	state.counter_prepare[clientid] = &newPrepareCounter
	state.counter_commit[clientid] = &newCommitCounter
	state.locks[clientid + "PREPARE"] = &sync.Mutex{}
	state.locks[clientid + "COMMIT"] = &sync.Mutex{}
}

func (state *PbftGlobalState) Run(
	message string, 
	id string, 
	clientid string,
	zone string,
	ch <-chan bool,
	broadcast func(string),

) bool {
	seq_num, _ := strconv.Atoi( strings.Split(message, "!")[1] )
	preprepare_msg := create_global_pbft_message(clientid, id, zone, "PRE_PREPARE",  message)

	state.counter_prepare[clientid].Count["0"] = 0
	state.counter_prepare[clientid].Count["1"] = 0
	state.counter_prepare[clientid].Count["2"] = 0
	state.counter_prepare[clientid].Count[zone] = 1
	state.counter_prepare[clientid].Seq = seq_num
	// state.counter_prepare.Store(message + "PREPARE", 1)

	go broadcast(preprepare_msg)
	committed := <- ch
	return committed
}

func (state *PbftGlobalState) HasQuorum(clientid string, msg_type string) bool {
	threshold := (2 * state.failures) + 1
	if msg_type == "PREPARE" {
		return state.counter_prepare[clientid].Count["0"] >= threshold && state.counter_prepare[clientid].Count["1"] >= threshold && state.counter_prepare[clientid].Count["2"] >= threshold
	} else {
		return state.counter_commit[clientid].Count["0"] >= threshold && state.counter_commit[clientid].Count["1"] >= threshold && state.counter_commit[clientid].Count["2"] >= threshold
	}
}

func (state *PbftGlobalState) HandleMessage(
	message string,
	broadcast func(string),
	id string,
	clientid string,
	my_zone string,
	sender_zone string,
	ch chan<- bool,
) {
	components := strings.Split(message, ";")
	if len(components) < 3 {
		return
	}
	msg_type := components[0]
	message_val := components[2]

	prepare_key := clientid + "PREPARE"
	commit_key := clientid + "COMMIT"

	seq_num, _ := strconv.Atoi(strings.Split(message_val, "!")[1])

	achieve_pbft_prepare_quorum := false
	// increment_amount := 1
	switch msg_type {


	case "PRE_PREPARE":
		s := create_global_pbft_message(clientid, id, my_zone, "PREPARE", message_val)
		
		// Vote for your own message
		state.locks[prepare_key].Lock()
		if seq_num < state.counter_prepare[clientid].Seq  {
			state.locks[prepare_key].Unlock()
			return
		} else if seq_num == state.counter_prepare[clientid].Seq {
			state.counter_prepare[clientid].Count[my_zone] += 1
		} else {
			state.counter_prepare[clientid].Seq = seq_num
			state.counter_prepare[clientid].Count["0"] = 0
			state.counter_prepare[clientid].Count["1"] = 0
			state.counter_prepare[clientid].Count["2"] = 0
			state.counter_prepare[clientid].Count[my_zone] = 1
		}

		state.locks[prepare_key].Unlock()
		go broadcast(s)
		fallthrough
	case "PREPARE":
		
		state.locks[prepare_key].Lock()
		if seq_num < state.counter_prepare[clientid].Seq  {
			state.locks[prepare_key].Unlock()
			return
		} else if seq_num == state.counter_prepare[clientid].Seq {
			state.counter_prepare[clientid].Count[sender_zone] += 1
		} else {
			state.counter_prepare[clientid].Seq = seq_num
			state.counter_prepare[clientid].Count["0"] = 0
			state.counter_prepare[clientid].Count["1"] = 0
			state.counter_prepare[clientid].Count["2"] = 0
			state.counter_prepare[clientid].Count[sender_zone] = 1
		}
		
		// interf, _ := state.counter_prepare.LoadOrStore(message_val + "PREPARE", 0)
		if state.HasQuorum(clientid, "PREPARE") {
			state.counter_prepare[clientid].Count["0"] = -30
			state.counter_prepare[clientid].Count["1"] = -30
			state.counter_prepare[clientid].Count["2"] = -30
			// state.counter_prepare.Store(message_val + "PREPARE", -30)
			state.locks[prepare_key].Unlock()	
			s := create_global_pbft_message(clientid, id, my_zone, "COMMIT", message_val)



			go broadcast(s)

			if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Printf("pbft prepare Quorum achieved for %s\n", message)
			}

			// Fallthrough to the next block and vote for your own commit message
			sender_zone = my_zone
			achieve_pbft_prepare_quorum = true
			
		} else {
			// state.counter_prepare.Store(message_val + "PREPARE", count + increment_amount)
			state.locks[prepare_key].Unlock()	
		}
		
	}
		// fmt.Printf("PREPARE_COUNT with key: %s : %v\n", message_val + "PREPARE", state.counter[message_val + "PREPARE"])
	if msg_type == "COMMIT" || achieve_pbft_prepare_quorum {

		state.locks[commit_key].Lock()

		if seq_num < state.counter_commit[clientid].Seq  {
			state.locks[commit_key].Unlock()
			return
		} else if seq_num == state.counter_commit[clientid].Seq {
			state.counter_commit[clientid].Count[sender_zone] += 1
		} else {
			state.counter_commit[clientid].Seq = seq_num
			state.counter_commit[clientid].Count["0"] = 0
			state.counter_commit[clientid].Count["1"] = 0
			state.counter_commit[clientid].Count["2"] = 0
			state.counter_commit[clientid].Count[sender_zone] = 1
		}

		if common.VERBOSE {
			fmt.Printf("COMMIT_COUNT key: %s : 0: %v, 1: %v, 2: %v\n", message_val, state.counter_commit[clientid].Count["0"], state.counter_commit[clientid].Count["1"], state.counter_commit[clientid].Count["2"])
		}
		// interf, _ := state.counter_commit.LoadOrStore(message_val + "COMMIT", 0)
		// count := interf.(int)
		if state.HasQuorum(clientid, "COMMIT") {
			state.counter_commit[clientid].Count["0"] = -30
			state.counter_commit[clientid].Count["1"] = -30
			state.counter_commit[clientid].Count["2"] = -30

			if common.VERBOSE {
				fmt.Printf("%s has been committed\n", message_val)
			}
			// state.counter_commit.Store(message_val + "COMMIT", -30)
			// Value has been committed
			state.locks[commit_key].Unlock()
			// Signal other channel			
			if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Printf("pbft commit Quorum achieved for pbft %s\n", message)
			}
			ch <- true
			
			
		} else {
			// state.counter_commit.Store(message_val + "COMMIT", count + 1)
			state.locks[commit_key].Unlock()
		}
	}

	
}
