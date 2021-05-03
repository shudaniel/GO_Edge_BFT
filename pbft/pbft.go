package pbft

import (
	"EdgeBFT/common"
	"strings"
	"sync"
	"strconv"
	"fmt"
)


type PbftState struct {
	counters_prepare map[string]map[string]int
	counters_commit map[string]map[string]int
	// counter_prepare           sync.Map
	// counter_commit           sync.Map
	failures int
	locks             map[string]*sync.Mutex
	//localLog          []common.Message
}

func create_pbft_message(clientid string, id string, msg_type string, message_val string) string {
	s := "PBFT|" + clientid + "|" + msg_type + ";" + id + ";" + message_val 
	return s
}

func NewPbftState(f int) *PbftState {
	newState := PbftState{
		counters_prepare: make(map[string]map[string]int),
		counters_commit: make(map[string]map[string]int),
		// counter_prepare:          	sync.Map{},
		// counter_commit:          	sync.Map{},
		locks:             make(map[string]*sync.Mutex),
		failures: f,
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}

func (state *PbftState) GetF() int {
	return state.failures
}

func (state *PbftState) Initialize(clientid string, num_t int ) {
	// fmt.Println("initialize pbft")
	state.counters_prepare[clientid] = make(map[string]int)
	state.counters_commit[clientid] = make(map[string]int)
	for i := 0; i < num_t; i++ {
		i_str := strconv.Itoa(i)
		state.counters_prepare[clientid][i_str] = 0
		state.counters_commit[clientid][i_str] = 0
	}
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
	seq_num := strings.Split(message, "!")[1]
	preprepare_msg := create_pbft_message(clientid, id,"PRE_PREPARE",  message)
	state.counters_prepare[clientid][seq_num] = 1
	// state.counters_prepare.Store(message + "PREPARE", 1)

	go broadcast(preprepare_msg)
	committed := <- ch
	return committed
}

func (state *PbftState) HandleMessage(
	message string,
	broadcast func(string),
	id string,
	clientid string,
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

	seq_num := strings.Split(message_val, "!")[1]

	achieve_pbft_prepare_quorum := false
	increment_amount := 1
	switch msg_type {


	case "PRE_PREPARE":
		s := create_pbft_message(clientid, id, "PREPARE", message_val)
		increment_amount += 1
		go broadcast(s)
		fallthrough
	case "PREPARE":
		
		state.locks[prepare_key].Lock()

		state.counters_prepare[clientid][seq_num] += increment_amount
		// interf, _ := state.counter_prepare.LoadOrStore(message_val + "PREPARE", 0)
		// count := interf.(int)
		if common.HasQuorum(state.counters_prepare[clientid][seq_num], state.failures) {
			state.counters_prepare[clientid][seq_num] = -30
			// state.counter_prepare.Store(message_val + "PREPARE", -30)
			state.locks[prepare_key].Unlock()	
			s := create_pbft_message(clientid, id, "COMMIT", message_val)
			go broadcast(s)

			if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Printf("pbft prepare Quorum achieved for %s\n", message)
			}
			achieve_pbft_prepare_quorum = true

			// state.locks[commit_key].Lock()
			// interf, _ = state.counter_commit.LoadOrStore(message_val + "COMMIT", 0)
			// state.counter_commit.Store(message_val + "COMMIT", interf.(int) + 1)
			// state.locks[commit_key].Unlock()
			
		} else {
			// state.counter_prepare.Store(message_val + "PREPARE", count + increment_amount)
			state.locks[prepare_key].Unlock()	
		}
		
	}
		// fmt.Printf("PREPARE_COUNT with key: %s : %v\n", message_val + "PREPARE", state.counter[message_val + "PREPARE"])
	if msg_type == "COMMIT" || achieve_pbft_prepare_quorum {

		state.locks[commit_key].Lock()
		state.counters_commit[clientid][seq_num]++
		// interf, _ := state.counter_commit.LoadOrStore(message_val + "COMMIT", 0)
		// count := interf.(int)
		if common.HasQuorum(state.counters_commit[clientid][seq_num], state.failures) {
			state.counters_commit[clientid][seq_num] = -30
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
