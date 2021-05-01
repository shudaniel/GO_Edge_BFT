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
	"crypto/rsa"
)

var lock_mutex = &sync.Mutex{}

type triple struct {
	Msg string
	Conn net.TCPConn
}

type node struct {
	directory		  map[string]map[string]net.TCPConn
	pbft_state        *pbft.PbftState
	endorse_state        *endorsement.EndorsementState
	paxos_state        *paxos.PaxosState
	id                string
	zone              string
	udpsock            *net.UDPConn
	msg_chan          chan triple
	endorse_signals           map[string]chan string
	pbft_signals           map[string]chan bool
	paxos_signals           map[string]chan bool
	public_keys            map[string]*rsa.PublicKey
	private_key            *rsa.PrivateKey
	client_list map[string]bool
	//localLog          []common.Message

}

func NewNode(ip string, port int, z string, f int) *node {


	priv_key, pub_key := common.GenerateKeyPair(0)


	newNode := node{
		directory:           make(map[string]map[string]net.TCPConn),
		pbft_state:          pbft.NewPbftState(f),
		endorse_state:       endorsement.NewEndorseState(f),
		paxos_state:          paxos.NewPaxosState(),
		zone:				 z,
		id: 				 ip + ":" + strconv.Itoa(port),
		msg_chan:           make(chan triple), 
		private_key:          priv_key,
		public_keys:         make(map[string]*rsa.PublicKey),
		endorse_signals:             make(map[string]chan string),
		pbft_signals:             make(map[string]chan bool),
		paxos_signals:             make(map[string]chan bool),

		client_list: make(map[string]bool),
	}
	newNode.public_keys[newNode.id] = pub_key


	for i := 0; i < 20; i++ { 
		new_msg_chan := make(chan triple)
		addr := net.TCPAddr{
			Port: port + i,
			IP: net.ParseIP(ip),
		}
		ser, err := net.ListenTCP("tcp", &addr)
		if err != nil {
			fmt.Printf("Some error %v\n", err)
			return nil
		}

		go newNode.listen(ser, new_msg_chan)
		go newNode.handlerRoutine(new_msg_chan)
	}

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

	i := 0
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
		addr := net.TCPAddr{
			Port: port + i,
			IP: net.ParseIP(line_components[0]),
		}

		i++
		c, err := net.DialTCP("tcp", nil, &addr)
        if err != nil {
            fmt.Println(err)
        } else {
			n.sendResponse(join_msg, *c)
			go n.handleConnection(*c, n.msg_chan)
		}

	}
	file.Close()
}

func (n *node) handlerRoutine(msg_chan chan triple) {
	var received_data triple
	for {
		received_data = <- msg_chan

		for _, value := range strings.Split(received_data.Msg, "*") {
			go n.handleMessage(value, received_data.Conn)
		}
		// go n.handleMessage(received_data.Msg, received_data.Conn)
	}
}

func (n *node) broadcastToZone(msg string) {
	if inner_dir, ok := n.directory[n.zone]; ok {
		for nodeid, conn := range inner_dir {
			if nodeid != n.id {
				n.sendResponse(msg, conn)
			}
		}
	}
}

func(n *node) sendToNode(msg string, nodeid string, zone string) {
	// Send message to one node in your zone, if node with nodeid exists
	if conn, ok := n.directory[zone][nodeid]; ok {
		n.sendResponse(msg, conn)
	}
}

func (n *node) handleJoin(message_components []string, conn net.TCPConn, reply bool) {
	nodeid := message_components[1]
	zone := message_components[2]
	pubkey := message_components[3]
	pubkey_bytes, err := hex.DecodeString(pubkey)
	if err != nil {
		fmt.Println(err)
	}

	lock_mutex.Lock()
	if _, ok := n.directory[zone]; !ok {
		n.directory[zone] = make(map[string]net.TCPConn)
	}
	n.public_keys[nodeid] = common.BytesToPublicKey(pubkey_bytes)
	n.directory[zone][nodeid] = conn
	lock_mutex.Unlock()
	
	if reply {
		reply_msg := n.createJoinMessage(false)
		n.sendResponse(reply_msg, conn)
	}
}

func (n *node) handleClientJoin(clientid string, zone string) {
	lock_mutex.Lock()
	if n.zone == zone {
		fmt.Printf("Client joining: %s\n", clientid)
		n.client_list[clientid] = true
	}
	n.pbft_signals[clientid] = make(chan bool)
	n.paxos_signals[clientid] = make(chan bool)
	n.endorse_signals[clientid] = make(chan string)
	fmt.Printf("Client locks created: %s\n", zone)
	n.pbft_state.Initialize(clientid)
	n.endorse_state.Initialize(clientid)
	n.paxos_state.Initialize(clientid)
	lock_mutex.Unlock()
}

func (n *node) handleClientRequest(message string, conn net.TCPConn) {
	components := strings.Split(message, "!")
	client_id := components[0]
	var success bool
	start := time.Now()
	if n.client_list[client_id] {
		// fmt.Println("%s is in client list", client_id)
		success = n.pbft_state.Run(message, n.id, client_id,  n.pbft_signals[client_id] ,n.broadcastToZone)
	} else {
		// fmt.Println("%s not is in client list", client_id)
		success = n.paxos_state.Run(message, n.id, n.zone, client_id, n.paxos_signals[client_id], n.broadcastInterzonal, n.broadcastToZone, n.endorse_signals, n.endorse_state)
	}
	end := time.Now()
	difference := end.Sub(start)
	total_time := difference.Seconds() 
	if !success {
		fmt.Println("FAILED on", message)
		total_time = 0.0
	} 
	n.sendResponse(fmt.Sprintf("%f", total_time), conn)
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

func (n *node) handleMessage(message string, conn net.TCPConn) {
	components := strings.Split(message, "|")
	// fmt.Printf("Received: %s \n", message)
	msg_type := components[0]
	switch msg_type {
	case "JOIN":
		fmt.Printf("Received: %s \n", message)
		n.handleJoin(components, conn, true)
	case "JOIN_NOREPLY":
		fmt.Printf("Received: %s \n", message)
		n.handleJoin(components, conn, false)
	case "CLIENT_JOIN":
		clientid := components[1]
		zone := components[2]
		n.handleClientJoin(clientid, zone)
	case "CLIENT_REQUEST":
		request_msg := components[1]
		n.handleClientRequest(request_msg, conn)
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

}

func (n *node)  handleConnection(c net.TCPConn, msg_chan chan triple) {
	// fmt.Printf("Serving %s\n", c.RemoteAddr().String())
	for {
			p := make([]byte, 8192)
			len, err := c.Read(p)
			// netData, err := bufio.NewReader(c).ReadString('*')
	
			if err == nil && len > 0 {

				msg_chan <- triple {
					Msg: strings.TrimSpace(string(p)),
					Conn: c,
				}
			}

			// result := strconv.Itoa(random()) + "\n"
			c.Write([]byte(string("hello")))
	}
	c.Close()
}

func (n *node) listen( sock *net.TCPListener, msg_chan chan triple ) {
	// for {
	// 
    //     _,remoteaddr,err := n.sock.ReadFromUDP(p)
    //     // fmt.Printf("Read a message (%d) %s \n", len, p)
	// 	n.msg_chan <- triple{
	// 		Msg: string( p ),
	// 		Address: remoteaddr,
	// 	}
    //     if err !=  nil {
    //         fmt.Printf("Some error  %v", err)
    //         continue
    //     }
    // }

	for {
        conn, err := sock.AcceptTCP()
        if err != nil {
            fmt.Println(err)
        }
        // fmt.Println("Calling handleConnection")
        go n.handleConnection(*conn, msg_chan)
    }
}

func (n *node) sendResponse(message string, c net.TCPConn) {
	// fmt.Printf("Sending: %s \n", message)

	
	// _,err := n.sock.WriteToUDP([]byte(message + "*"), addr)
	_,err := c.Write([]byte(string(message + "*")))
	if err != nil {
		fmt.Printf("Couldn't send response %v", err)
	}
	
}

func (n *node) Run() {
	time.Sleep(20 * time.Second)
	n.joinNetwork()

	// Wait here forever
	finished := make(chan bool)
	<- finished
}