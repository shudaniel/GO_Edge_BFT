package main
import (
    // "net"  
	"fmt"
	"EdgeBFT/node"
	"os"
	"strconv"
)



// func sendResponse(conn *net.UDPConn, addr *net.UDPAddr) {
//     _,err := conn.WriteToUDP([]byte("From server: Hello I got your message "), addr)
//     if err != nil {
//         fmt.Printf("Couldn't send response %v", err)
//     }
// }


func main() {
	fmt.Println("Hello")
	ip_addr := "127.0.0.1"
	port := 8000
	zone := "0"
	f := 1

	test := make(map[string]int)
	test["hello?"]++

	fmt.Printf("%v\n", test["hello?"])

	argsWithoutProg := os.Args[1:]
	for i, s := range argsWithoutProg {
		switch s {
		case "-f":
			new_f, err := strconv.Atoi(argsWithoutProg[i + 1])
			if err == nil {
				f = new_f
			}
		
		case "-a":
			ip_addr = argsWithoutProg[i + 1]
		
		case "-p":
			new_p, err := strconv.Atoi(argsWithoutProg[i + 1])
			if err == nil {
				port = new_p
			}
		case "-z":
			zone = argsWithoutProg[i + 1]
		}

	}

	node := node.NewNode(ip_addr, port, zone, f)
	node.Run()
    // p := make([]byte, 2048)
    // addr := net.UDPAddr{
    //     Port: 8000,
    //     IP: net.ParseIP("127.0.0.1"),
    // }
    // ser, err := net.ListenUDP("udp", &addr)
    // if err != nil {
    //     fmt.Printf("Some error %v\n", err)
    //     return
    // }
	// fmt.Printf("ready\n")
    // for {
    //     _,remoteaddr,err := ser.ReadFromUDP(p)
    //     fmt.Printf("Read a message from %v %s \n", remoteaddr, p)
	// 	pbft_node.HandleMessage(p)
    //     if err !=  nil {
    //         fmt.Printf("Some error  %v", err)
    //         continue
    //     }
    //     go sendResponse(ser, remoteaddr)
    // }
}