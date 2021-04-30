package node
import (
    "fmt" 
    "net"  
	"EdgeBFT/pbft"
	"EdgeBFT/endorsement"
	"EdgeBFT/paxos"
	"strconv"
	"strings"
	"os"
	"bufio"
)

type triple struct {
	Msg string
	Address *net.UDPAddr
}

type node struct {
	directory		  map[string]map[string]*net.UDPAddr
	pbft_state        *pbft.PbftState
	endorse_state        *endorsement.EndorsementState
	paxos_state        *paxos.PaxosState
	sock              *net.UDPConn
	id                string
	zone              string
	msg_chan          chan triple
	endorse_signals           map[string]chan bool
	pbft_signals           map[string]chan bool
	paxos_signals           map[string]chan bool
	//localLog          []common.Message
}

func NewNode(ip string, port int, z string, f int) *node {

	addr := net.UDPAddr{
        Port: port,
        IP: net.ParseIP(ip),
    }
    ser, err := net.ListenUDP("udp", &addr)
    if err != nil {
        fmt.Printf("Some error %v\n", err)
        return nil
    }


	newNode := node{
		directory:           make(map[string]map[string]*net.UDPAddr),
		pbft_state:          pbft.NewPbftState(f),
		endorse_state:       endorsement.NewEndorseState(f),
		paxos_state:          paxos.NewPaxosState(),
		zone:				 z,
		id: 				 ip + ":" + strconv.Itoa(port),
		sock:                ser,
		msg_chan:           make(chan triple),
		endorse_signals:             make(map[string]chan bool),
		pbft_signals:             make(map[string]chan bool),
		paxos_signals:             make(map[string]chan bool),
	}

	return &newNode
}

func (n *node) reset() {
	fmt.Println("RESET")
	n.pbft_state = pbft.NewPbftState( n.pbft_state.GetF() )
	n.endorse_state = endorsement.NewEndorseState(n.endorse_state.GetF())
	n.paxos_state = paxos.NewPaxosState()

	n.endorse_signals = make(map[string]chan bool)
	n.pbft_signals = make(map[string]chan bool)
	n.paxos_signals = make(map[string]chan bool)

}

func (n *node) joinNetwork() {
	file, err := os.Open("addresses.txt")
	if err != nil {
		fmt.Println("Error opening addresses")
		fmt.Println(err)
		return
	}

	join_msg := "JOIN|" + n.id + "|" + n.zone
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
	var received_data triple
	for {
		received_data = <- n.msg_chan
		for _, value := range strings.Split(received_data.Msg, "*") {
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

func(n *node) sendToNode(msg string, nodeid string) {
	// Send message to one node in your zone, if node with nodeid exists
	if addr, ok := n.directory[n.zone][nodeid]; ok {
		n.sendResponse(msg, addr)
	}
}

func (n *node) handleJoin(message_components []string, addr *net.UDPAddr, reply bool) {
	nodeid := message_components[1]
	zone := message_components[2]

	if _, ok := n.directory[zone]; !ok {
		n.directory[zone] = make(map[string]*net.UDPAddr)
		
	}
	
	n.directory[zone][nodeid] = addr
	
	
	if reply {
		reply_msg := "JOIN_NOREPLY|" + n.id + "|" + n.zone
		n.sendResponse(reply_msg, addr)
	}
}

func (n *node) handleClientRequest(message string) {

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
	fmt.Printf("Received: %s \n", message)
	msg_type := components[0]
	switch msg_type {
	case "JOIN":
		n.handleJoin(components, addr, true)
	case "JOIN_NOREPLY":
		n.handleJoin(components, addr, false)
	case "ENDORSE":
		endorse_msg := components[1]
		go n.endorse_state.HandleMessage(endorse_msg, n.broadcastToZone, n.sendToNode, n.id, n.endorse_signals)
	case "PAXOS":
		paxos_msg := components[1]
		go n.paxos_state.HandleMessage(paxos_msg, n.broadcastInterzonal, n.broadcastToZone, n.id, n.paxos_signals, n.endorse_signals, n.endorse_state)
	case "PBFT":
		pbft_msg := components[1]
		go n.pbft_state.HandleMessage(pbft_msg, n.broadcastToZone ,n.id, n.pbft_signals)
	case "RESET":
		n.reset()
	}

	// go n.sendResponse(ser, remoteaddr)
}

func (n *node) listen() {
	p := make([]byte, 2048)
	for {
        _,remoteaddr,err := n.sock.ReadFromUDP(p)
        // fmt.Printf("Read a message from %v %s \n", remoteaddr, p)
		n.msg_chan <- triple{
			Msg: string( p[:] ),
			Address: remoteaddr,
		}
        if err !=  nil {
            fmt.Printf("Some error  %v", err)
            continue
        }
    }
}

func (n *node) sendResponse(message string, addr *net.UDPAddr) {
	
	_,err := n.sock.WriteToUDP([]byte(message + "*"), addr)
	if err != nil {
		fmt.Printf("Couldn't send response %v", err)
	}
	
}

func (n *node) Run() {
	n.joinNetwork()
	go n.listen()
	go n.handlerRoutine()

	// Wait here forever
	finished := make(chan bool)
	<- finished
}