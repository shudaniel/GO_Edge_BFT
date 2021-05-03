package endorsement

import (
	"EdgeBFT/common"
	"strings"
	"sync"
	"crypto/rsa"
	"encoding/hex"
	"fmt"
	"strconv"
)

type EndorsementState struct {
	// counter_prepare map[string]*Counter
	
	counter_prepare           map[string]*common.Counter
	counter_promise           map[string]*common.Counter
	signatures                map[string][]string
	failures          int
	locks             map[string]*sync.Mutex
}

func NewEndorseState(f int) *EndorsementState {
	newState := EndorsementState{
		counter_prepare:          	make(map[string]*common.Counter),
		counter_promise:          	make(map[string]*common.Counter),
		signatures: make(map[string][]string),
		locks:  make(map[string]*sync.Mutex),
		failures: f,
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}

func createEndorseMsg(msg_type string, message string, nodeid string, original_senderid string, clientid string) string {
	return "ENDORSE|"  + msg_type + ";" + nodeid +  ";" + original_senderid + ";" + clientid + ";" + message 
}

func (state *EndorsementState) GetF() int {
	return state.failures
}

func (state *EndorsementState) Initialize(clientid string ) {
	// fmt.Println("initialize endorsement")
	newPrepareCounter := common.Counter {
		Seq: -1,
		Count: 0,
	}
	newPromiseCounter := common.Counter {
		Seq: -1,
		Count: 0,
	}
	state.counter_prepare[clientid] = &newPrepareCounter
	state.counter_promise[clientid] = &newPromiseCounter

	state.signatures[clientid] = []string{"", "", ""}
	state.locks[clientid + "E_PREPARE"] = &sync.Mutex{}
	state.locks[clientid + "E_PROMISE"] = &sync.Mutex{}
}


func (state *EndorsementState) Run(
	message string, 
	id string, 
	clientid string,
	ch <-chan string,
	broadcast func(string),

) string {

	seq, _ := strconv.Atoi( strings.Split(message, "!")[1] )
	preprepare_msg := createEndorseMsg("E_PRE_PREPARE", message, id, id, clientid)
	state.counter_prepare[clientid].Seq = seq
	state.counter_prepare[clientid].Count = 1

	// state.counter_prepare.Store(message  + "E_PREPARE", 1)
	
	// fmt.Printf("E_PREPARE_COUNT before sending preprepares with key: %s : %v\n", message + "E_PREPARE", state.counter[message + "E_PREPARE"])
	go broadcast(preprepare_msg)
	signatures := <-ch


	return signatures	
}

func (state *EndorsementState) HandleMessage(
	message string,
	broadcast func(string),
	sendMessage func(string, string, string),
	zone string,
	id string,
	signals map[string]chan string,
	public_keys map[string]*rsa.PublicKey,
	priv *rsa.PrivateKey,
) {
	components := strings.Split(message, ";")
	msg_type := components[0]
	nodeid := components[1]
	original_senderid := components[2]
	clientid := components[3]
	msg_value := components[4]

	seq_num, _ := strconv.Atoi(strings.Split(msg_value, "!")[1])

	prepare_key := clientid + "E_PREPARE"
	promise_key := clientid + "E_PROMISE"

	achieve_prepare_quorum := false
	signature_str := ""
	increment_amount := 1
	switch msg_type {


	case "E_PRE_PREPARE":
		s := createEndorseMsg( "E_PREPARE", msg_value, id, original_senderid, clientid )
		increment_amount++
		broadcast(s)
		fallthrough
	case "E_PREPARE":
		
		state.locks[prepare_key].Lock()

		if seq_num < state.counter_prepare[clientid].Seq  {
			state.locks[prepare_key].Unlock()
			return
		} else if seq_num == state.counter_prepare[clientid].Seq {
			state.counter_prepare[clientid].Count += increment_amount
		} else {
			state.counter_prepare[clientid].Seq = seq_num
			state.counter_prepare[clientid].Count = increment_amount
		}

		// interf, _ := state.counter_prepare.LoadOrStore(msg_value + "E_PREPARE", 0)
		// count := interf.(int) 
		if common.HasQuorum(state.counter_prepare[clientid].Count, state.failures) {
			state.counter_prepare[clientid].Count = -30
			// state.counter_prepare.Store(msg_value + "E_PREPARE", -30)
			state.locks[prepare_key].Unlock()

			// Sign the original value and send back

			signed_msg := common.SignWithPrivateKey( []byte(msg_value), priv)
			signature_str = hex.EncodeToString(signed_msg)
			// fmt.Println("Signed", msg_value, "by", id, ". LENGTH:", len(msg_value))
			s := createEndorseMsg( "E_PROMISE", msg_value, id, original_senderid, clientid ) + ";" + signature_str
			sendMessage(s, original_senderid, zone)
			achieve_prepare_quorum = true

			// state.locks[promise_key].Lock()
			// interf, _ = state.counter_promise.LoadOrStore(msg_value + "E_PROMISE", 0)
			// state.counter_promise.Store(msg_value + "E_PROMISE", interf.(int) + 1)
			// state.locks[promise_key].Unlock()
			if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Printf("Quorum endorsement achieved, message signed %s\n", message)
			}
			
		} else {
			// state.counter_prepare.Store(msg_value + "E_PREPARE", count + increment_amount)
			state.locks[prepare_key].Unlock()
		}

	case "E_PROMISE":
		// signature := components[4] 
		// First, verify the message
		cipher, err := hex.DecodeString(components[5])
		if err != nil || !common.VerifyWithPublicKey([]byte(msg_value), cipher, public_keys[nodeid] ) {
			if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Println("Failed verification for", msg_value, "from", nodeid, ". LENGTH:", len(msg_value))
			}
			return
		}
		signature_str = components[5]
	}
	if msg_type == "E_PROMISE" || achieve_prepare_quorum {

		i := state.counter_promise[clientid].Count 
		state.locks[promise_key].Lock()
		if seq_num < state.counter_promise[clientid].Seq  {
			state.locks[prepare_key].Unlock()
			return
		} else if seq_num == state.counter_promise[clientid].Seq {
			state.counter_promise[clientid].Count += 1
		} else {
			state.counter_promise[clientid].Seq = seq_num
			state.counter_promise[clientid].Count = 1
		}

		// interf, _ := state.counter_promise.LoadOrStore(msg_value + "E_PROMISE", 0)
		// count := interf.(int) 
		if common.HasQuorum(state.counter_promise[clientid].Count, state.failures) {

			state.signatures[clientid][i] = signature_str
			state.counter_promise[clientid].Count = -30
			// state.counter_promise.Store(msg_value + "E_PROMISE", -30)
			signatures_str := strings.Join(state.signatures[clientid], "/")
			state.signatures[clientid][0] = ""
			state.signatures[clientid][1] = ""
			state.signatures[clientid][2] = ""
			state.locks[promise_key].Unlock()

			// Value has been committed
			// Signal other channel
			if ch, ok := signals[clientid]; ok {
				if common.VERBOSE && common.VERBOSE_EXTRA {
					fmt.Printf("Quorum promise achieved for endorsement %s\n", message)
				}
				ch <- signatures_str
			}
			// Endorsement achieved
		} else {
			// state.counter_promise.Store(msg_value + "E_PROMISE", count + 1)
			if i >= 0 && i < len(state.signatures[clientid]) { 
				state.signatures[clientid][i] = signature_str
			}
			state.locks[promise_key].Unlock()
		}

	}
}
