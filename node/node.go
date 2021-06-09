package node
import (
    "fmt" 
    "net"  
	"EdgeBFT/pbft"
	"EdgeBFT/tracker"
	"EdgeBFT/endorsement"
	"EdgeBFT/common"
	"encoding/hex"
	"encoding/json"
	"EdgeBFT/paxos"
	"strconv"
	"strings"
	"os"
	"time"
	"bufio"
	"sync"
	"math/rand"
	"crypto/rsa"
	"runtime"
	"regexp"
	"github.com/libp2p/go-reuseport"
)

var lock_mutex = &sync.Mutex{}
var isValidString = regexp.MustCompile(`[a-zA-Z0-9_:!|.;,~/{}"\[\] ]*`) 

type IncomingTCPMessage struct {
	Msg string
	outbox chan string
}

type IncomingUDPMessage struct {
	Msg []byte
	Address *net.UDPAddr
}

type OutgoingUDPMessage struct {
    recipient *net.UDPAddr
    data      string
}

type CompletedTxn struct {
	latency float64
	clientid string
}

type node struct {
	my_addr           string
	directory		  map[string]map[string]chan string
	pbft_state        *pbft.PbftState
	pbft_global_state        *pbft.PbftGlobalState
	endorse_state        *endorsement.EndorsementState
	paxos_state        *paxos.PaxosState
	sock              *net.UDPConn
	id                string
	zone              string
	inboxUDP          chan IncomingUDPMessage
	outboxesTCP           map[string]chan string
	outboxUDP            chan OutgoingUDPMessage
	endorse_signals           map[string]chan string
	pbft_signals           map[string]chan bool
	global_signals           map[string]chan bool
	public_keys            map[string]*rsa.PublicKey
	private_key            *rsa.PrivateKey
	client_list map[string]bool
	txn_input chan CompletedTxn
	//localLog          []common.Message
}

func NewNode(ip string, port int, z string, f int) *node {

	randbits := rand.Intn(100)

	priv_key, pub_key := common.GenerateKeyPair(randbits)


	newNode := node{

		my_addr:            ip + ":" + strconv.Itoa(port),
		directory:           make(map[string]map[string]chan string),
		pbft_state:          pbft.NewPbftState(f),
		pbft_global_state:          pbft.NewPbftGlobalState(f),
		endorse_state:       endorsement.NewEndorseState(f),
		paxos_state:          paxos.NewPaxosState(),
		zone:				 z,
		id: 				 ip + ":" + strconv.Itoa(port),
		outboxUDP:               make(chan OutgoingUDPMessage, common.MAX_CHANNEL_SIZE),
		outboxesTCP:      make(map[string]chan string),
		inboxUDP:           make(chan IncomingUDPMessage, common.MAX_CHANNEL_SIZE),
		private_key:          priv_key,
		public_keys:         make(map[string]*rsa.PublicKey),
		endorse_signals:             make(map[string]chan string),
		pbft_signals:             make(map[string]chan bool),
		global_signals:             make(map[string]chan bool),
		txn_input: make(chan CompletedTxn, common.MAX_CHANNEL_SIZE),
		client_list: make(map[string]bool),
	}
	newNode.public_keys[newNode.id] = pub_key

	return &newNode
}

func (n *node) reset() {
	fmt.Println("RESET")
	// n.pbft_state = pbft.NewPbftState( n.pbft_state.GetF() )
	// n.endorse_state = endorsement.NewEndorseState(n.endorse_state.GetF())
	// n.paxos_state = paxos.NewPaxosState()

	n.endorse_signals = make(map[string]chan string)
	n.pbft_signals = make(map[string]chan bool)
	n.global_signals = make(map[string]chan bool)

}

func (n *node) createJoinMessage(reply bool) string {
	// Send over publickey as well

	
	lock_mutex.Lock()
	pubkey_bytes := common.PublicKeyToBytes( n.public_keys[n.id] )
	lock_mutex.Unlock()
	pubkey_str := hex.EncodeToString(pubkey_bytes)

	msg := n.id + "|" + n.zone + "|" + pubkey_str 
	if reply {
		msg = "JOIN|" + msg
	} else {
		msg = "JOIN_NOREPLY|" + msg
	}

	return msg
}

func (n *node) joinNetwork() {
	file, err := os.Open("addresses.txt")
	if err != nil {
		fmt.Println("Error opening addresses")
		fmt.Println(err)
		return
	}

	// join_msg := n.createJoinMessage(true)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line_components := strings.Split(line, " ")
		addr :=  line_components[0] + ":" + line_components[1]
		if addr == n.my_addr {
			// Don't connect to yourself
			continue
		}
		c, err := net.Dial("tcp", addr)
        if err != nil {
                fmt.Println(err)
                continue
        }
		outbox := make(chan string, common.MAX_CHANNEL_SIZE)
		go n.handleConnection(c, outbox)

	}
	file.Close()
}

func (n *node) udpHandlerRoutine() {
	// var received_data IncomingUDPMessage
	// for {
	// 	received_data = <- n.inboxUDP
	// 	for _, value := range strings.Split(strings.TrimSpace(string(received_data.Msg)), common.MESSAGE_DELIMITER) {
	// 		go n.handleUDPMessage(value, received_data.Address)
	// 	}

	// }

	var received_data IncomingUDPMessage
	message := ""
	for {
		received_data = <- n.inboxUDP
		for _, value := range strings.Split(strings.TrimSpace(string(received_data.Msg)), common.MESSAGE_DELIMITER) {
			matches := isValidString.FindAllString(value, -1)
			for _, v := range matches {
				if len(v) > 0  {
					// Check if the end of the message is "end." Otherwise this is a partial message and you must wait for the rest
					if v[len(value)-1:] == common.MESSAGE_ENDER {
						go n.handleUDPMessage(message + v, received_data.Address)
						message = ""
					} else {
						message = message + v
						// fmt.Println("Message so far ************************************************************************\n", message)
					}
					
				}
			}
		}
		
		

	}
}


func (n *node) broadcastToZone(msg string) {
	// fmt.Println("Broadcast to zone:", msg)
	if inner_dir, ok := n.directory[n.zone]; ok {
		for nodeid, outbox := range inner_dir {
			if nodeid != n.id {
				// fmt.Println("Nodeid:", nodeid)
				n.sendTCPResponse(msg, outbox)
			}
		}
	}
}

func(n *node) sendToNode(msg string, nodeid string, zone string) {
	// Send message to one node in your zone, if node with nodeid exists
	if outbox, ok := n.directory[zone][nodeid]; ok {
		n.sendTCPResponse(msg, outbox)
	}
}

func (n *node) handleJoin(message_components []string, outbox chan string, reply bool) {
	nodeid := message_components[1]
	zone := message_components[2]
	pubkey := message_components[3]
	pubkey_bytes, err := hex.DecodeString(pubkey)
	if err != nil {
		fmt.Println(err)
	}

	lock_mutex.Lock()
	if _, ok := n.directory[zone]; !ok {
		n.directory[zone] = make(map[string]chan string)
	}
	n.public_keys[nodeid] = common.BytesToPublicKey(pubkey_bytes)
	n.directory[zone][nodeid] = outbox
	lock_mutex.Unlock()
	
}

func (n *node) handleClientJoin(startingid int, zone string, num_c int) {

	for i := 0; i < num_c; i++ {
		clientid := strconv.Itoa(startingid + i)
		lock_mutex.Lock()
		if n.zone == zone {
			fmt.Printf("Client joining: %s\n", clientid)
			n.client_list[clientid] = true
		} else {
			n.client_list[clientid] = false
		}
		n.pbft_signals[clientid] = make(chan bool, common.MAX_CHANNEL_SIZE)
		n.global_signals[clientid] = make(chan bool, common.MAX_CHANNEL_SIZE)
		n.endorse_signals[clientid] = make(chan string, common.MAX_CHANNEL_SIZE)
		fmt.Printf("Client locks created: %s\n", zone)
		n.pbft_state.Initialize(clientid)
		n.pbft_global_state.Initialize(clientid)
		n.endorse_state.Initialize(clientid)
		n.paxos_state.Initialize(clientid)
		lock_mutex.Unlock()
	}
}

func (n *node) handleClientRequest(message string, outbox chan string) {
	components := strings.Split(message, "!")
	client_id := components[0]

	results := make(chan bool)
	txn_type := "l"

	total_time := 0.0
	var end time.Time
	start := time.Now()
	if n.client_list[client_id] {
		// fmt.Println("%s is in client list", client_id)
		go func(message string, id string, client_id string, ch chan bool, broadcast func(string), result chan bool) {

			success := n.pbft_state.Run(message, id, client_id,  ch , broadcast)
			result <- success

		} (message, n.id, client_id,  n.pbft_signals[client_id] ,n.broadcastToZone, results)
		
	} else {
		txn_type = "g"
		if common.GLOBAL_TYPE == "PBFT" {
			go func(message string, id string, client_id string, zone string, ch chan bool, broadcast func(string), result chan bool) {

				success := n.pbft_global_state.Run(message, id, client_id, zone, ch , broadcast)
				result <- success

			} (message, n.id, client_id, n.zone,  n.global_signals[client_id] ,n.broadcastEveryone, results)

		} else {

		
			go func(message string, id string, zone string, client_id string, ch <-chan bool, broadcast func(string), localbroadcast func(string), endorse_signals map[string]chan string, state *endorsement.EndorsementState, result chan bool) {
				
				success := n.paxos_state.Run(message, id, zone, client_id, ch, broadcast, localbroadcast, endorse_signals, state)
				result <- success

			} (message, n.id, n.zone, client_id, n.global_signals[client_id], n.broadcastInterzonal, n.broadcastToZone, n.endorse_signals, n.endorse_state, results)
		// fmt.Println("%s not is in client list", client_id)
		}
	}
	select {
    case <-results:
		end = time.Now()
		difference := end.Sub(start)
		total_time = difference.Seconds() 

		n.txn_input <- CompletedTxn{
			latency: total_time,
			clientid: client_id,
		}
		if common.VERBOSE && common.VERBOSE_EXTRA {
			fmt.Println("Got response from paxos for ", message, txn_type, total_time)
		}
    case <-time.After(common.TIMEOUT * time.Second):
		
		fmt.Println("TIMEOUT on", message, txn_type)
		
    }
	
	n.sendTCPResponse(fmt.Sprintf("%f,%d,%d", total_time, start.UnixNano(), end.UnixNano()), outbox)
	// fmt.Println("Total time: %d", total_time)

}

func (n *node) broadcastInterzonal(message string) {
	for zone, _ := range n.directory {
		if zone != n.zone {
			for _, outbox := range n.directory[zone] {
				
				n.sendTCPResponse(message, outbox)
				break
				
			}
		}

	}
}

func (n *node) broadcastEveryone(message string) {
	// Broadcast a message to everyone
	for zone, _ := range n.directory {
		for _, outbox := range n.directory[zone] {
			
			n.sendTCPResponse(message, outbox)
			
		}
	}
}

func (n *node) handleTCPMessage(message string, outbox chan string) {
	components := strings.Split(message, "|")
	if common.VERBOSE {
		fmt.Printf("Received: %d %s \n", len(message), message)
	}
	msg_type := components[0]
	switch msg_type {
	case "JOIN":
		fmt.Printf("Received: %s \n", message)
		n.handleJoin(components, outbox, true)
	case "JOIN_NOREPLY":
		fmt.Printf("Received: %s \n", message)
		n.handleJoin(components, outbox, false)
	case "ENDORSE":
		endorse_msg := components[1]
		n.endorse_state.HandleMessage(endorse_msg, n.broadcastToZone, n.sendToNode, n.zone, n.id, n.endorse_signals, n.public_keys, n.private_key)
	case "PAXOS":
		paxos_msg := components[1]
		n.paxos_state.HandleMessage(paxos_msg, n.broadcastInterzonal, n.broadcastToZone, n.sendToNode, n.id, n.global_signals, n.endorse_signals, n.endorse_state)
	case "SHARE":
		paxos_msg := components[1]
		n.paxos_state.HandleShareMessage(paxos_msg)
	case "PBFT":
		clientid := components[1]
		pbft_msg := components[2]
		n.pbft_state.HandleMessage(pbft_msg, n.broadcastToZone ,n.id, clientid, n.pbft_signals[clientid])
	case "PBFT_GLOBAL":
		clientid := components[1]
		senderzone := components[2]
		pbft_msg := components[3]
		n.pbft_global_state.HandleMessage(pbft_msg, n.broadcastEveryone, n.id, clientid, n.zone, senderzone, n.global_signals[clientid])
	case "TXN_DATA":
		txn_json_data := components[1]
		go n.RunClientTracker(n.zone, txn_json_data, outbox)
	case "CLIENT_REQUEST":
		request_msg := components[1]
		n.handleClientRequest(request_msg, outbox)
		
	}
}

func (n *node) handleUDPMessage(message string, addr *net.UDPAddr) {
	components := strings.Split(message, "|")
	if common.VERBOSE {
		fmt.Printf("Received: %s \n", message)	
	}
	msg_type := components[0]
	switch msg_type {
	case "CLIENT_JOIN":
		clientid, _ :=  strconv.Atoi(components[1])
		zone := components[2]
		num_c, _ := strconv.Atoi(components[3])
		n.handleClientJoin(clientid, zone, num_c)
	// case "TXN_DATA":
	// 	txn_json_data := components[1]
	// 	go n.RunClientTracker(n.zone, txn_json_data, addr)
	case "RESET":
		n.reset()
	}
}

func (n *node)  RunClientTracker(zone string, txn_json string, outbox chan string) {
	type ClientJsondata struct {
		Zone string
		Clientid string
		Numtxn int
	}

	if common.VERBOSE && common.VERBOSE_EXTRA { 
		fmt.Println("Received txn_json", txn_json)
	}
	var clientTxnInfo []ClientJsondata
	byt := []byte(txn_json)
	tracker := tracker.NewClientsTracker()
	if err := json.Unmarshal(byt, &clientTxnInfo); err != nil {
        fmt.Println(err)
		return
    }

	// fmt.Println("clienttTxnInfo:", clientTxnInfo)
	// Count the total number of transactions you expect
	num_total_transactions := 0
	for i := 0; i < len(clientTxnInfo); i++ {
		if clientTxnInfo[i].Zone == n.zone {
			num_total_transactions += clientTxnInfo[i].Numtxn
			tracker.AddClient( clientTxnInfo[i].Clientid )
		}
	}

	fmt.Println("Expecting", num_total_transactions, "txns")

	// Wait to get the transaction data
	for i := 0; i < num_total_transactions; i++ {
		data := <-n.txn_input
		tracker.AddLatency(data.clientid, data.latency)	
	}
	
	fmt.Println("Done with txns, going to send back resposne now")
	// Send the response back 
	responsedata := tracker.GenerateReturnData()
	n.sendTCPResponse(responsedata, outbox)
	// n.sendUDPResponse(responsedata, addr)
}


func (n *node) handleConnection(c net.Conn, outbox chan string) {
	fmt.Printf("Serving %s\n", c.RemoteAddr().String())

	// On connect, send a JOIN message
	join_msg := n.createJoinMessage(false) + "|" + common.MESSAGE_ENDER + common.MESSAGE_DELIMITER
	c.Write([]byte(join_msg))


	sendFromOutbox := func(c net.Conn, outbox chan string) {
		var message string
		// var msg_bytes []byte 
		for {
			message = <- outbox
			c.Write([]byte(message))
		}
	}

	parseMessage := func(n *node, inbox chan IncomingTCPMessage ) {
		var received_data IncomingTCPMessage
		// message := ""
		for {
			received_data = <- inbox
			// value := string(received_data.Msg)
			value := received_data.Msg
			if common.VERBOSE && common.VERBOSE_EXTRA {
				fmt.Println("Raw Received", value)
			}
			// if value[len(value)-1:] == common.MESSAGE_ENDER {
			go n.handleTCPMessage(value, received_data.outbox)
			// }
			// matches := isValidString.FindAllString(value, -1)
			// for _, v := range matches {
			// 	if len(v) > 0 {
			// 		// Check if the end of the message is "end." Otherwise this is a partial message and you must wait for the rest
			// 		if v[len(v)-1:] == common.MESSAGE_ENDER {
			// 			go n.handleTCPMessage(message + v, received_data.outbox)
			// 			message = ""
			// 		} else {
			// 			message = message + v
			// 		}
					
			// 	}
			// }
			
			
			

		}
	}

	

	// conn := bufio.NewReader(c)

	for i := 1;  i <= 4; i++ {
        go sendFromOutbox(c, outbox)
    }

	inbox := make(chan IncomingTCPMessage, common.MAX_CHANNEL_SIZE)
	go parseMessage(n,inbox)	

	reader := bufio.NewReader(c)
	
	for {
			// p := make([]byte, 1024)
			p, err := reader.ReadString('*')
			if err != nil {
				if err.Error() == "EOF" {
					// fmt.Println("EOF detected")
					break
				}
					
			} else {
				inbox<- IncomingTCPMessage{
					Msg: p,
					outbox: outbox,
				}
			}
	}
	c.Close()
}

func (n *node) sendTCPResponse(message string, outbox chan string) {
	if common.VERBOSE {
		fmt.Println("Sending", message)
	}
	outbox <- (message + "|" + common.MESSAGE_ENDER + common.MESSAGE_DELIMITER)
}

func (n *node) listenTCP() {
	ser, err := reuseport.Listen("tcp", n.my_addr)
    if err != nil {
        fmt.Printf("Some error %v\n", err)
        return 
    }
	defer ser.Close()

	for {
		c, err := ser.Accept()
		if err != nil {
				fmt.Println(err)
		} else {
			outbox := make(chan string, common.MAX_CHANNEL_SIZE)
			go n.handleConnection(c, outbox)
		}
	}

}

func (n *node) listenUDP() {
	ser, err := reuseport.ListenPacket("udp", n.my_addr)
    if err != nil {
        fmt.Printf("Some error %v\n", err)
        return 
    }

	// outbox := make(chan OutgoingUDPMessage, common.MAX_CHANNEL_SIZE)

	sendFromOutbox := func( outbox chan OutgoingUDPMessage ) {
        n, err := 0, error(nil)
        for msg := range outbox {
			start := 0


			// Send in 2048 byte chunks
			for start + 2046 < len(msg.data) {
				n, err = ser.(*net.UDPConn).WriteToUDP([]byte( common.MESSAGE_DELIMITER + msg.data[start:(start + 2046)] + common.MESSAGE_DELIMITER ), msg.recipient)
				if err != nil {
					fmt.Println(err)
				}
				if n != 2048 {
					fmt.Println("Tried to send", len(msg.data[start:(start + 2046)]) + 2, "bytes but only sent ", n)
				}

				start += 2048
				// Sleep a little to ensure the message arrives in order
				time.Sleep(400 * time.Millisecond)
			}
			n, err = ser.(*net.UDPConn).WriteToUDP([]byte(common.MESSAGE_DELIMITER + msg.data[start:] + common.MESSAGE_DELIMITER ), msg.recipient)
			if err != nil {
				fmt.Println(err)
			}
			if n != (len(msg.data[start:]) + 2) {
				fmt.Println("Tried to send", len(msg.data[start:]) + 2, "bytes but only sent ", n)
			}

        }
    }

	go sendFromOutbox(n.outboxUDP)

	for {
		p := make([]byte, 1024)
        len,remoteaddr,err := ser.(*net.UDPConn).ReadFromUDP(p)
		if common.VERBOSE_EXTRA {
			fmt.Printf("Read UDP message (%d) %s \n", len, p)
		}
		n.inboxUDP <- IncomingUDPMessage{
			Msg: p,
			Address: remoteaddr,
		}
        if err !=  nil {
            fmt.Printf("Some error  %v", err)
            continue
        }
    }
}

func (n *node) sendUDPResponse(message string, addr *net.UDPAddr) {
	// fmt.Printf("Sending: %s \n", message)
	// Place the message on an outbox
	n.outboxUDP <- OutgoingUDPMessage{
		recipient: addr,
		data: (message + common.MESSAGE_ENDER + common.MESSAGE_DELIMITER),
	}
	
}

func (n *node) Run() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	for i:= 0; i < runtime.NumCPU(); i++ {

		go n.listenTCP()
	}

	go n.listenUDP()
	go n.udpHandlerRoutine()

	n.joinNetwork()

	// Wait here forever
	finished := make(chan bool)
	<- finished
}