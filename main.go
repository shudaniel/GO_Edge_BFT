package main
import (
    // "net"  
	"EdgeBFT/node"
)



// func sendResponse(conn *net.UDPConn, addr *net.UDPAddr) {
//     _,err := conn.WriteToUDP([]byte("From server: Hello I got your message "), addr)
//     if err != nil {
//         fmt.Printf("Couldn't send response %v", err)
//     }
// }


func main() {

	ip_addr := "127.0.0.1"
	port := 8000
	zone := "0"
	f := 1

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