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
	"regexp"
	// "unicode"
	"github.com/libp2p/go-reuseport"
)

var lock_mutex = &sync.Mutex{}
var isValidString = regexp.MustCompile(`^[a-zA-Z0-9_:!|.;,~/]*$`).MatchString 

type IncomingTCPMessage struct {
	Msg []byte
	outbox chan string
}


type IncomingUDPMessage struct {
	Msg []byte
	Address *net.UDPAddr
}

type OutgoingUDPMessage struct {
    recipient *net.UDPAddr
    data      []byte
}

type node struct {
	my_addr           string
	directory		  map[string]map[string]chan string
	pbft_state        *pbft.PbftState
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
	paxos_signals           map[string]chan bool
	public_keys            map[string]*rsa.PublicKey
	private_key            *rsa.PrivateKey
	client_list map[string]bool
	//localLog          []common.Message
}

func NewNode(ip string, port int, z string, f int) *node {

	randbits := rand.Intn(100)

	// addr := net.UDPAddr{
    //     Port: port,
    //     IP: net.ParseIP(ip),
    // }
    // ser, err := reuseport.ListenPacket("udp", addr.String())
    // if err != nil {
    //     fmt.Printf("Some error %v\n", err)
    //     return nil
    // }

	priv_key, pub_key := common.GenerateKeyPair(randbits)


	newNode := node{

		my_addr:            ip + ":" + strconv.Itoa(port),
		directory:           make(map[string]map[string]chan string),
		pbft_state:          pbft.NewPbftState(f),
		endorse_state:       endorsement.NewEndorseState(f),
		paxos_state:          paxos.NewPaxosState(),
		zone:				 z,
		id: 				 ip + ":" + strconv.Itoa(port),
		// inboxTCP:        make(chan IncomingTCPMessage, common.MAX_CHANNEL_SIZE),
		outboxUDP:               make(chan OutgoingUDPMessage, common.MAX_CHANNEL_SIZE),
		outboxesTCP:      make(map[string]chan string),
		// sock:                ser,
		inboxUDP:           make(chan IncomingUDPMessage, common.MAX_CHANNEL_SIZE),
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
		// port, err := strconv.Atoi(line_components[1])
		// if err != nil {
		// 	file.Close()
		// 	fmt.Println("Error with port")
		// 	fmt.Println(err)
		// 	return
		// }
		// addr := net.UDPAddr{
		// 	Port: port,
		// 	IP: net.ParseIP(line_components[0]),
		// }
		
	}
	file.Close()
}

func (n *node) udpHandlerRoutine() {
	var received_data IncomingUDPMessage
	for {
		received_data = <- n.inboxUDP
		for _, value := range strings.Split(strings.TrimSpace(string(received_data.Msg)), common.MESSAGE_DELIMITER) {
			go n.handleUDPMessage(value, received_data.Address)
		}

	}
}

// func (n *node) tcpHandlerRoutine() {
// 	var received_data IncomingTCPMessage
// 	for {
// 		received_data = <- n.inboxTCP

// 		for _, value := range strings.Split(strings.TrimSpace(string(received_data.Msg)), "*") {
// 			if len(value) > 0 {
// 				// Check that the end of the message is what you expect, otherwise you need to wait for the rest of it
// 				go n.handleTCPMessage(value, received_data.outbox)
// 			}
// 			// if value[0] < unicode.MaxASCII {
// 			// }
// 		}
		
		

// 	}
// }

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
	
// 	if reply {
// 		reply_msg := n.createJoinMessage(false)
// 		n.sendTCPResponse(reply_msg, nodeid)
// 	}
}

func (n *node) handleClientJoin(startingid int, zone string, num_c int) {

	for i := 0; i < num_c; i++ {
		clientid := strconv.Itoa(startingid + i)
		lock_mutex.Lock()
		if n.zone == zone {
			fmt.Printf("Client joining: %s\n", clientid)
			n.client_list[clientid] = true
		}
		n.pbft_signals[clientid] = make(chan bool, common.MAX_CHANNEL_SIZE)
		n.paxos_signals[clientid] = make(chan bool, common.MAX_CHANNEL_SIZE)
		n.endorse_signals[clientid] = make(chan string, common.MAX_CHANNEL_SIZE)
		fmt.Printf("Client locks created: %s\n", zone)
		n.pbft_state.Initialize(clientid)
		n.endorse_state.Initialize(clientid)
		n.paxos_state.Initialize(clientid)
		lock_mutex.Unlock()
	}
}

func (n *node) handleClientRequest(message string, outbox chan string) {
	components := strings.Split(message, "!")
	client_id := components[0]

	ch := make(chan bool)
	txn_type := "l"

	total_time := 0.0
	end := time.Now()
	start := time.Now()
	if n.client_list[client_id] {
		// fmt.Println("%s is in client list", client_id)
		go func(message string, id string, client_id string, ch chan bool, broadcast func(string), result chan bool) {

			success := n.pbft_state.Run(message, id, client_id,  ch , broadcast)
			result <- success

		} (message, n.id, client_id,  n.pbft_signals[client_id] ,n.broadcastToZone, ch)
		
	} else {
		txn_type = "g"
		go func(message string, id string, zone string, client_id string, ch <-chan bool, broadcast func(string), localbroadcast func(string), endorse_signals map[string]chan string, state *endorsement.EndorsementState, result chan bool) {

			success := n.paxos_state.Run(message, id, zone, client_id, ch, broadcast, localbroadcast, endorse_signals, state)
			result <- success

		} (message, n.id, n.zone, client_id, n.paxos_signals[client_id], n.broadcastInterzonal, n.broadcastToZone, n.endorse_signals, n.endorse_state, ch)
		// fmt.Println("%s not is in client list", client_id)
		// success = n.paxos_state.Run(message, n.id, n.zone, client_id, n.paxos_signals[client_id], n.broadcastInterzonal, n.broadcastToZone, n.endorse_signals, n.endorse_state)
	}
	select {
    case <-ch:
		end = time.Now()
		difference := end.Sub(start)
		total_time = difference.Seconds() 
		if common.VERBOSE && common.VERBOSE_EXTRA {
			fmt.Println("Got response from paxos for ", message, txn_type, total_time)
		}
    case <-time.After(common.TIMEOUT * time.Second):
		
		fmt.Println("TIMEOUT on", message, txn_type)
		
    }
	
	// n.sendUDPResponse(fmt.Sprintf("%f", total_time), addr)
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
		n.paxos_state.HandleMessage(paxos_msg, n.broadcastInterzonal, n.broadcastToZone, n.sendToNode, n.id, n.paxos_signals, n.endorse_signals, n.endorse_state)
	case "SHARE":
		paxos_msg := components[1]
		n.paxos_state.HandleShareMessage(paxos_msg)
	case "PBFT":
		clientid := components[1]
		pbft_msg := components[2]
		n.pbft_state.HandleMessage(pbft_msg, n.broadcastToZone ,n.id, clientid, n.pbft_signals[clientid])

	case "CLIENT_REQUEST":
		request_msg := components[1]
		n.handleClientRequest(request_msg, outbox)

	}
}

func (n *node) handleUDPMessage(message string, addr *net.UDPAddr) {
	components := strings.Split(message, "|")
	// fmt.Printf("Received: %s \n", message)
	msg_type := components[0]
	switch msg_type {
	case "CLIENT_JOIN":
		clientid, _ :=  strconv.Atoi(components[1])
		zone := components[2]
		num_c, _ := strconv.Atoi(components[3])
		n.handleClientJoin(clientid, zone, num_c)

	case "RESET":
		n.reset()
	}
}


func (n *node) handleConnection(c net.Conn, outbox chan string) {
	fmt.Printf("Serving %s\n", c.RemoteAddr().String())

	// On connect, send a JOIN message
	join_msg := common.MESSAGE_DELIMITER + n.createJoinMessage(false) + "|" + common.MESSAGE_ENDER + common.MESSAGE_DELIMITER
	c.Write([]byte(join_msg))

	// msg_bytes := append([]byte(join_msg), "\n"...)
	// c.Write(msg_bytes)

	sendFromOutbox := func(c net.Conn, outbox chan string) {
		var message string
		// var msg_bytes []byte 
		for {
			message = <- outbox
			c.Write([]byte(message))
			
			// msg_bytes = append([]byte(message), "\n"...)

			// err := c.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
			// if err != nil {
			// 	outbox <- message
			// 	continue
			// }
			// // if CheckConnError(err, c) { return }
			// _, err = c.Write(msg_bytes)
			// // if CheckConnError(err, c) { return }
			// // len, _ := c.Write([]byte(message))
			// // fmt.Println("Wrote", len, "bytes")
		}
	}

	parseMessage := func(n *node, inbox chan IncomingTCPMessage ) {
		var received_data IncomingTCPMessage
		message := ""
		for {
			received_data = <- inbox
			for _, value := range strings.Split(strings.TrimSpace(string(received_data.Msg)), common.MESSAGE_DELIMITER) {
				if common.VERBOSE && common.VERBOSE_EXTRA {
					fmt.Println("Raw Received", value)
				}
				if len(value) > 0 && isValidString(value) {
					// Check if the end of the message is "end." Otherwise this is a partial message and you must wait for the rest
					if value[len(value)-1:] == common.MESSAGE_ENDER {
						go n.handleTCPMessage(message + value, received_data.outbox)
						message = ""
					} else {
						message = message + value
					}
					
				}
			}
			
			

		}
	}

	

	// conn := bufio.NewReader(c)

	for i := 1;  i <= 4; i++ {
        go sendFromOutbox(c, outbox)
    }

	inbox := make(chan IncomingTCPMessage, common.MAX_CHANNEL_SIZE)
	go parseMessage(n,inbox)	
	
	for {
			p := make([]byte, 1024)
			_, err := c.Read(p)
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

			// message, err := bufio.NewReader(c).ReadString('\n')
			// if err != nil {
			// 	if err.Error() == "EOF" {
			// 		fmt.Println("EOF detected")
			// 		break
			// 	}
					
			// } else {
			// 	n.inboxTCP<- IncomingTCPMessage{
			// 		Msg: message,
			// 		outbox: outbox,
			// 	}
			// }


			// buff := make([]byte, 1024)
			// // read a single byte which contains the message length
			// size, err := conn.ReadByte()
			// if err != nil {
			// 	fmt.Println("error reading byte:", err)
			// 	continue
			// }

			// // read the full message, or return an error
			// _, err := io.ReadFull(conn, buff[:int(size)])
			// if err != nil {
			// 	fmt.Println("error reading full", err)
			// 	continue
			// }
			// n.inboxTCP<- IncomingTCPMessage{
			// 	Msg: buff,
			// 	outbox: outbox,
			// }

		
			// fmt.Print("-> ", string(netData))
	}
	c.Close()
}

func (n *node) sendTCPResponse(message string, outbox chan string) {
	if common.VERBOSE {
		fmt.Println("Sending", message)
	}
	outbox <- (common.MESSAGE_DELIMITER + message + "|" + common.MESSAGE_ENDER + common.MESSAGE_DELIMITER)
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
        go sendFromOutbox(n.outboxUDP)
    }

	for {
		p := make([]byte, 1024)
        _,remoteaddr,err := ser.(*net.UDPConn).ReadFromUDP(p)
        // fmt.Printf("Read a message (%d) %s \n", len, p)
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
		data: []byte(message + common.MESSAGE_DELIMITER),
	}
	// _,err := n.sock.WriteToUDP([]byte(message + "*"), addr)
	// if err != nil {
	// 	fmt.Printf("Couldn't send response %v", err)
	// }
	
}

func (n *node) Run() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	for i:= 0; i < runtime.NumCPU(); i++ {
		go n.listenUDP()
		go n.udpHandlerRoutine()

		go n.listenTCP()
		// go n.tcpHandlerRoutine()
	}

	n.joinNetwork()

	// Wait here forever
	finished := make(chan bool)
	<- finished
}