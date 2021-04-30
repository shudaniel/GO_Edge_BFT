package pbft

import (
	"EdgeBFT/common"
	"strings"
	"sync"
)


type PbftState struct {
	counter_prepare           sync.Map
	counter_commit           sync.Map
	failures int
	locks             map[string]*sync.Mutex
	//localLog          []common.Message
}

func create_pbft_message(clientid string, id string, msg_type string, message_val string) string {
	s := "PBFT|" + clientid + "|" + msg_type + ";" + id + ";" + message_val + ";end"
	return s
}

func NewPbftState(f int) *PbftState {
	newState := PbftState{
		counter_prepare:          	sync.Map{},
		counter_commit:          	sync.Map{},
		locks:             make(map[string]*sync.Mutex),
		failures: f,
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}

func (state *PbftState) GetF() int {
	return state.failures
}

func (state *PbftState) Initialize(clientid string ) {
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

	preprepare_msg := create_pbft_message(clientid, id,"PRE_PREPARE",  message)
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
	msg_type := components[0]
	message_val := components[2]

	prepare_key := clientid + "PREPARE"
	commit_key := clientid + "COMMIT"
	switch msg_type {


	case "PRE_PREPARE":

		state.locks[prepare_key].Lock()
		interf, _ := state.counter_prepare.LoadOrStore(message_val + "PREPARE", 0)
		count := interf.(int) 
		if common.HasQuorum(count + 1, state.failures) {
			state.counter_prepare.Store(message_val + "PREPARE", -30)
			state.locks[prepare_key].Unlock()	
			
			s := create_pbft_message(clientid, id, "COMMIT", message_val)
			// fmt.Printf("Quorum achieved for %s\n", message)
			state.locks[commit_key].Lock()
			interf, _ = state.counter_commit.LoadOrStore(message_val + "COMMIT", 0)
			state.counter_commit.Store(message_val + "COMMIT", interf.(int) + 1)
			// state.counter[message_val + "COMMIT"]++
			state.locks[commit_key].Unlock()
			go broadcast(s)
		} else {
			state.counter_prepare.Store(message_val + "PREPARE", count + 1)
			state.locks[prepare_key].Unlock()	
		}
		s := create_pbft_message(clientid, id, "PREPARE", message_val)
		go broadcast(s)
		// fmt.Printf("PREPARE_COUNT with key: %s : %v\n", message_val + "PREPARE", state.counter[message_val + "PREPARE"])
		

	case "PREPARE":
		
		state.locks[prepare_key].Lock()
		interf, _ := state.counter_prepare.LoadOrStore(message_val + "PREPARE", 0)
		count := interf.(int)
		// state.counter[message_val + "PREPARE"]++
		if common.HasQuorum(count + 1, state.failures) {
			state.counter_prepare.Store(message_val + "PREPARE", -30)
		// if common.HasQuorum(state.counter[message_val + "PREPARE"], state.failures) {
			// state.counter[message_val + "PREPARE"] = -30
			state.locks[prepare_key].Unlock()	
			s := create_pbft_message(clientid, id, "COMMIT", message_val)
			// fmt.Printf("Quorum achieved for %s\n", message)
			state.locks[commit_key].Lock()
			interf, _ = state.counter_commit.LoadOrStore(message_val + "COMMIT", 0)
			state.counter_commit.Store(message_val + "COMMIT", interf.(int) + 1)
			// state.counter[message_val + "COMMIT"]++
			state.locks[commit_key].Unlock()
			go broadcast(s)
		} else {
			state.counter_prepare.Store(message_val + "PREPARE", count + 1)
			state.locks[prepare_key].Unlock()	
		}
		// fmt.Printf("PREPARE_COUNT with key: %s : %v\n", message_val + "PREPARE", state.counter[message_val + "PREPARE"])
	case "COMMIT":

		state.locks[commit_key].Lock()
		interf, _ := state.counter_commit.LoadOrStore(message_val + "COMMIT", 0)
		count := interf.(int)
		// state.counter[message_val + "COMMIT"]++
		if common.HasQuorum(count + 1, state.failures) {
			state.counter_commit.Store(message_val + "COMMIT", -30)
		// if common.HasQuorum(state.counter[message_val + "COMMIT"], state.failures) {
			// state.counter[message_val + "COMMIT"] = -30
			// Value has been committed
			state.locks[commit_key].Unlock()
			// Signal other channel
			
			// fmt.Printf("Quorum achieved for pbft %s\n", message)
			ch <- true
			
		} else {
			state.counter_commit.Store(message_val + "COMMIT", count + 1)
			state.locks[commit_key].Unlock()
		}

	}
}
