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
	signals           map[string]chan string
	in_use  map[string]*bool
}

func NewEndorseState(f int) *EndorsementState {
	newState := EndorsementState{
		counter_prepare:          	make(map[string]*common.Counter),
		counter_promise:          	make(map[string]*common.Counter),
		signatures: make(map[string][]string),
		locks:  make(map[string]*sync.Mutex),
		signals: make(map[string]chan string),
		failures: f,
		in_use: make(map[string]*bool),
		//localLog:          make([]common.Message, 0),
	}

	return &newState
}

func createEndorseMsg(msg_type string, message string, nodeid string, original_senderid string, clientid string, seq_num int) string {
	return "ENDORSE|"  + msg_type + ";" + nodeid +  ";" + original_senderid + ";" + clientid + ";" + message + ";" + strconv.Itoa(seq_num)
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

	false_bool := false
	state.in_use[clientid] = &false_bool

	state.counter_prepare[clientid] = &newPrepareCounter
	state.counter_promise[clientid] = &newPromiseCounter

	state.signatures[clientid] = []string{"", "", ""}
	state.signals[clientid] = make(chan string)
	state.locks[clientid] = &sync.Mutex{}
	// state.locks[clientid + "E_PROMISE"] = &sync.Mutex{}
}


func (state *EndorsementState) Run(
	message string, 
	seq int,
	id string, 
	clientid string,
	broadcast func(string),

) string {

	return "WORKS"
	
	// seq, _ := strconv.Atoi( strings.Split(message, "!")[1] )
	preprepare_msg := createEndorseMsg("E_PRE_PREPARE", message, id, id, clientid, seq)

	// fmt.Printf("Run %s, seq is now %s\n", message, strconv.Itoa(seq))
	state.locks[clientid].Lock()
	if *state.in_use[clientid] {
		// There is something else trying to get the endorsement right now, so we need to kill it
		state.signals[clientid] <- "/"
	}	
	*state.in_use[clientid] = true
	// state.locks[clientid + "E_PROMISE"].Lock()
	state.counter_prepare[clientid].Seq = seq
	state.counter_prepare[clientid].Count = 1

	state.counter_promise[clientid].Seq = seq
	state.counter_promise[clientid].Count = 0
	// state.locks[clientid + "E_PREPARE"].Unlock()
	state.locks[clientid].Unlock()

	// state.counter_prepare.Store(message  + "E_PREPARE", 1)
	
	// fmt.Printf("E_PREPARE_COUNT before sending preprepares with key: %s : %v\n", message + "E_PREPARE", state.counter[message + "E_PREPARE"])
	go broadcast(preprepare_msg)

	signatures := <-state.signals[clientid]
	if common.VERBOSE && common.VERBOSE_EXTRA {
		fmt.Printf("Finished endorsement %s\n", message)
	}

	return signatures	
}

func (state *EndorsementState) HandleMessage(
	message string,
	broadcast func(string),
	sendMessage func(string, string, string),
	zone string,
	id string,
	public_keys map[string]*rsa.PublicKey,
	priv *rsa.PrivateKey,
) {
	components := strings.Split(message, ";")
	msg_type := components[0]
	nodeid := components[1]
	original_senderid := components[2]
	clientid := components[3]
	msg_value := components[4]

	seq_num, _ := strconv.Atoi(components[5])
	// fmt.Printf("Seq num for %s is %s\n", message, seq_num)

	// prepare_key := clientid + "E_PREPARE"
	// promise_key := clientid + "E_PROMISE"

	achieve_prepare_quorum := false
	signature_str := ""
	increment_amount := 1
	switch msg_type {


	case "E_PRE_PREPARE":
		s := createEndorseMsg( "E_PREPARE", msg_value, id, original_senderid, clientid, seq_num )
		increment_amount++
		broadcast(s)
		fallthrough
	case "E_PREPARE":
		
		state.locks[clientid].Lock()
		fmt.Printf("Received Prepare %s, seq is now %s\n", message, strconv.Itoa(state.counter_prepare[clientid].Seq))
		if seq_num < state.counter_prepare[clientid].Seq  {
			state.locks[clientid].Unlock()
			// fmt.Printf("Bad prepare seq num for %s, %s < %s \n", message, seq_num, state.counter_prepare[clientid].Seq)
			return
		} else if seq_num == state.counter_prepare[clientid].Seq {
			state.counter_prepare[clientid].Count += increment_amount
		} else {

			// fmt.Printf("Received Prepare %s, seq iss now %s\n", message, strconv.Itoa(seq_num))
			
			state.counter_prepare[clientid].Seq = seq_num
			state.counter_prepare[clientid].Count = increment_amount
		}

		fmt.Println("Prepare Message vote count", state.counter_prepare[clientid].Count, message,  common.HasQuorum(state.counter_prepare[clientid].Count, state.failures) )

		if common.HasQuorum(state.counter_prepare[clientid].Count, state.failures) {
			state.counter_prepare[clientid].Count = -30

			if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Printf("Quorum endorsement achieved, message signed %s\n", message)
			}
			
			state.locks[clientid].Unlock()

			// Sign the original value and send back

			signed_msg := common.SignWithPrivateKey( []byte(msg_value), priv)
			signature_str = hex.EncodeToString(signed_msg)
			

			fmt.Printf("%s, OG ID: %s, myid: %s\n", msg_value, original_senderid, id)
			if original_senderid != id {
				s := createEndorseMsg( "E_PROMISE", msg_value, id, original_senderid, clientid, seq_num ) + ";" + signature_str
				sendMessage(s, original_senderid, zone)
			} else {
				// Don't send a message to yourself. Move to the next block 
				state.locks[clientid].Lock()
				achieve_prepare_quorum = true	
			}

			
		} else {
			// state.counter_prepare.Store(msg_value + "E_PREPARE", count + increment_amount)
			state.locks[clientid].Unlock()
		}

	case "E_PROMISE":
		// signature := components[4] 
		signature_str = components[6]
		// First, verify the message
		cipher, err := hex.DecodeString(signature_str)
		if err != nil || !common.VerifyWithPublicKey([]byte(msg_value), cipher, public_keys[nodeid] ) {
			if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Println("Failed verification for", msg_value, "from", nodeid, ". LENGTH:", len(msg_value))
			}
			return
		}
		
	}
	if msg_type == "E_PROMISE" || achieve_prepare_quorum {
		if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Println("Handling promise message for", msg_value)
			}
		if !achieve_prepare_quorum {
			state.locks[clientid].Lock()
		}

		if seq_num < state.counter_promise[clientid].Seq  {
			state.locks[clientid].Unlock()
			// fmt.Printf("Bad promise seq num for %s, %s < %s \n", message, seq_num, state.counter_promise[clientid].Seq)
			return
		} else if seq_num == state.counter_promise[clientid].Seq {
			state.counter_promise[clientid].Count += 1
		} else {
			// fmt.Printf("Received Commit %s, seq is now %s\n", message, strconv.Itoa(seq_num))

			state.counter_promise[clientid].Seq = seq_num
			state.counter_promise[clientid].Count = 1
		}
		fmt.Println("Promise Message vote count", state.counter_promise[clientid].Count, message )
		i := state.counter_promise[clientid].Count - 1
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

			if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Printf("Quorum promise achieved for endorsement %s\n", message)
			}

			// Value has been committed
			// Signal other channel
			// if ch, ok := signals[clientid]; ok {
			// 	ch <- signatures_str
			// }
			// Endorsement achieved
			state.signals[clientid] <- signatures_str + ";"
			*state.in_use[clientid] = false
			state.locks[clientid].Unlock()
		} else {
			// state.counter_promise.Store(msg_value + "E_PROMISE", count + 1)
			if i >= 0 && i < len(state.signatures[clientid]) { 
				state.signatures[clientid][i] = signature_str
			}
			state.locks[clientid].Unlock()
		}

	}
}
