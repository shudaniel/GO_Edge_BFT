package node
import (
    "fmt" 
    "net"  
	"EdgeBFT/pbft"
	"EdgeBFT/endorsement"
	"EdgeBFT/common"
	"encoding/hex"
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
	"github.com/libp2p/go-reuseport"
)

var lock_mutex = &sync.Mutex{}

type IncomingMessage struct {
	Msg []byte
	Address *net.UDPAddr
}

type OutgoingMessage struct {
    recipient *net.UDPAddr
    data      []byte
}


type node struct {
	my_addr           net.UDPAddr
	directory		  map[string]map[string]*net.UDPAddr
	pbft_state        *pbft.PbftState
	endorse_state        *endorsement.EndorsementState
	paxos_state        *paxos.PaxosState
	sock              *net.UDPConn
	id                string
	zone              string
	inbox          chan IncomingMessage
	outbox            chan OutgoingMessage
	endorse_signals           map[string]chan string
	pbft_signals           map[string]chan bool
	paxos_signals           map[string]chan bool
	public_keys            map[string]*rsa.PublicKey
	private_key            *rsa.PrivateKey
	client_list map[string]bool
	//localLog          []common.Message
}

func NewNode(ip string, port int, z string, f int) *node {

	randbits := rand.Intn(100)

	addr := net.UDPAddr{
        Port: port,
        IP: net.ParseIP(ip),
    }
    // ser, err := reuseport.ListenPacket("udp", addr.String())
    // if err != nil {
    //     fmt.Printf("Some error %v\n", err)
    //     return nil
    // }

	priv_key, pub_key := common.GenerateKeyPair(randbits)


	newNode := node{

		my_addr:            addr,
		directory:           make(map[string]map[string]*net.UDPAddr),
		pbft_state:          pbft.NewPbftState(f),
		endorse_state:       endorsement.NewEndorseState(f),
		paxos_state:          paxos.NewPaxosState(),
		zone:				 z,
		id: 				 ip + ":" + strconv.Itoa(port),
		outbox:               make(chan OutgoingMessage, common.MAX_CHANNEL_SIZE),
		// sock:                ser,
		inbox:           make(chan IncomingMessage, common.MAX_CHANNEL_SIZE),
		private_key:          priv_key,
		public_keys:         make(map[string]*rsa.PublicKey),
		endorse_signals:             make(map[string]chan string),
		pbft_signals:             make(map[string]chan bool),
		paxos_signals:             make(map[string]chan bool),

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
	n.paxos_signals = make(map[string]chan bool)

}

func (n *node) createJoinMessage(reply bool) string {
	// Send over publickey as well

	pubkey_bytes := common.PublicKeyToBytes( n.public_keys[n.id] )
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

	join_msg := n.createJoinMessage(true)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line_components := strings.Split(line, " ")
		port, err := strconv.Atoi(line_components[1])
		if err != nil {
			file.Close()
			fmt.Println("Error with port")
			fmt.Println(err)
			return
		}
		addr := net.UDPAddr{
			Port: port,
			IP: net.ParseIP(line_components[0]),
		}
		n.sendResponse(join_msg, &addr)
	}
	file.Close()
}

func (n *node) handlerRoutine() {
	var received_data IncomingMessage
	for {
		received_data = <- n.inbox
		for _, value := range strings.Split(strings.TrimSpace(string(received_data.Msg)), "*") {
			go n.handleMessage(value, received_data.Address)
		}

	}
}

func (n *node) broadcastToZone(msg string) {
	if inner_dir, ok := n.directory[n.zone]; ok {
		for nodeid, addr := range inner_dir {
			if nodeid != n.id {
				n.sendResponse(msg, addr)
			}
		}
	}
}

func(n *node) sendToNode(msg string, nodeid string, zone string) {
	// Send message to one node in your zone, if node with nodeid exists
	if addr, ok := n.directory[zone][nodeid]; ok {
		n.sendResponse(msg, addr)
	}
}

func (n *node) handleJoin(message_components []string, addr *net.UDPAddr, reply bool) {
	nodeid := message_components[1]
	zone := message_components[2]
	pubkey := message_components[3]
	pubkey_bytes, err := hex.DecodeString(pubkey)
	if err != nil {
		fmt.Println(err)
	}

	lock_mutex.Lock()
	if _, ok := n.directory[zone]; !ok {
		n.directory[zone] = make(map[string]*net.UDPAddr)
	}
	n.public_keys[nodeid] = common.BytesToPublicKey(pubkey_bytes)
	n.directory[zone][nodeid] = addr
	lock_mutex.Unlock()
	
	if reply {
		reply_msg := n.createJoinMessage(false)
		n.sendResponse(reply_msg, addr)
	}
}

func (n *node) handleClientJoin(clientid string, zone string) {
	lock_mutex.Lock()
	if n.zone == zone {
		fmt.Printf("Client joining: %s\n", clientid)
		n.client_list[clientid] = true
	}
	// n.pbft_signals[clientid] = make(chan bool, common.MAX_CHANNEL_SIZE)
	// n.paxos_signals[clientid] = make(chan bool, common.MAX_CHANNEL_SIZE)
	// n.endorse_signals[clientid] = make(chan string, common.MAX_CHANNEL_SIZE)
	fmt.Printf("Client locks created: %s\n", zone)
	n.pbft_state.Initialize(clientid)
	n.endorse_state.Initialize(clientid)
	n.paxos_state.Initialize(clientid)
	lock_mutex.Unlock()
}

func (n *node) handleClientRequest(message string, addr *net.UDPAddr) {
	components := strings.Split(message, "!")
	client_id := components[0]
	var success bool
	start := time.Now()
	ch := make(chan bool)
	lock_mutex.Lock()
	n.pbft_signals[clientid] = make(chan bool)
	n.paxos_signals[clientid] = make(chan bool)
	n.endorse_signals[clientid] = make(chan string)
	lock_mutex.Unlock()
	
	if n.client_list[client_id] {
		// fmt.Println("%s is in client list", client_id)
		go func(message string, id string, client_id string, ch chan bool, broadcast func(string), result chan bool) {

			success := n.pbft_state.Run(message, id, client_id,  ch , broadcast)
			result <- success

		} (message, n.id, client_id,  n.pbft_signals[client_id] ,n.broadcastToZone, ch)
		
	} else {
		go func(message string, id string, zone string, client_id string, ch <-chan bool, broadcast func(string), localbroadcast func(string), endorse_signals map[string]chan string, state *endorsement.EndorsementState, result chan bool) {

			success := n.paxos_state.Run(message, id, zone, client_id, ch, broadcast, localbroadcast, endorse_signals, state)
			result <- success

		} (message, n.id, n.zone, client_id, n.paxos_signals[client_id], n.broadcastInterzonal, n.broadcastToZone, n.endorse_signals, n.endorse_state, ch)
		// fmt.Println("%s not is in client list", client_id)
		// success = n.paxos_state.Run(message, n.id, n.zone, client_id, n.paxos_signals[client_id], n.broadcastInterzonal, n.broadcastToZone, n.endorse_signals, n.endorse_state)
	}
	select {
    case success = <-ch:
        break
		
    case <-time.After(common.TIMEOUT * time.Second):
       success = false
    }
	end := time.Now()
	difference := end.Sub(start)
	total_time := difference.Seconds() 
	
	if !success {
		fmt.Println("FAILED on", message)
		total_time = 0.0
	} 
	n.sendResponse(fmt.Sprintf("%f", total_time), addr)
	// fmt.Println("Total time: %d", total_time)

}

func (n *node) broadcastInterzonal(message string) {
	for zone, _ := range n.directory {
		if zone != n.zone {
			for _, addr := range n.directory[zone] {
				n.sendResponse(message, addr)
				break
			}
		}

	}
}

func (n *node) handleMessage(message string, addr *net.UDPAddr) {
	components := strings.Split(message, "|")
	// fmt.Printf("Received: %s \n", message)
	msg_type := components[0]
	switch msg_type {
	case "JOIN":
		fmt.Printf("Received: %s \n", message)
		n.handleJoin(components, addr, true)
	case "JOIN_NOREPLY":
		fmt.Printf("Received: %s \n", message)
		n.handleJoin(components, addr, false)
	case "CLIENT_JOIN":
		clientid := components[1]
		zone := components[2]
		n.handleClientJoin(clientid, zone)
	case "CLIENT_REQUEST":
		request_msg := components[1]
		n.handleClientRequest(request_msg, addr)
	case "ENDORSE":
		endorse_msg := components[1]
		n.endorse_state.HandleMessage(endorse_msg, n.broadcastToZone, n.sendToNode, n.zone, n.id, n.endorse_signals, n.public_keys, n.private_key)
	case "PAXOS":
		paxos_msg := components[1]
		n.paxos_state.HandleMessage(paxos_msg, n.broadcastInterzonal, n.broadcastToZone, n.sendToNode, n.id, n.paxos_signals, n.endorse_signals, n.endorse_state)
	case "SHARE":
		paxos_msg := components[1]
		n.paxos_state.HandleShareMessage(paxos_msg)
	case "PBFT":
		clientid := components[1]
		pbft_msg := components[2]
		n.pbft_state.HandleMessage(pbft_msg, n.broadcastToZone ,n.id, clientid, n.pbft_signals[clientid])
	case "RESET":
		n.reset()
	}

	// go n.sendResponse(ser, remoteaddr)
}

func (n *node) listen() {
	ser, err := reuseport.ListenPacket("udp", n.my_addr.String())
    if err != nil {
        fmt.Printf("Some error %v\n", err)
        return 
    }

	// outbox := make(chan OutgoingMessage, common.MAX_CHANNEL_SIZE)

	sendFromOutbox := func( outbox chan OutgoingMessage ) {
        n, err := 0, error(nil)
        for msg := range outbox {
            n, err = ser.(*net.UDPConn).WriteToUDP(msg.data, msg.recipient)
            if err != nil {
                fmt.Println(err)
            }
            if n != len(msg.data) {
                fmt.Println("Tried to send", len(msg.data), "bytes but only sent ", n)
            }
        }
    }

	for i := 1;  i <= 4; i++ {
        go sendFromOutbox(n.outbox)
    }

	for {
		p := make([]byte, 1024)
        _,remoteaddr,err := ser.(*net.UDPConn).ReadFromUDP(p)
        // fmt.Printf("Read a message (%d) %s \n", len, p)
		n.inbox <- IncomingMessage{
			Msg: p,
			Address: remoteaddr,
		}
        if err !=  nil {
            fmt.Printf("Some error  %v", err)
            continue
        }
    }
}

func (n *node) sendResponse(message string, addr *net.UDPAddr) {
	// fmt.Printf("Sending: %s \n", message)
	// Place the message on an outbox
	n.outbox <- OutgoingMessage{
		recipient: addr,
		data: []byte(message + "*"),
	}
	// _,err := n.sock.WriteToUDP([]byte(message + "*"), addr)
	// if err != nil {
	// 	fmt.Printf("Couldn't send response %v", err)
	// }
	
}

func (n *node) Run() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	for i:= 0; i < runtime.NumCPU(); i++ {
		go n.listen()
		go n.handlerRoutine()
	}

	n.joinNetwork()

	// Wait here forever
	finished := make(chan bool)
	<- finished
}