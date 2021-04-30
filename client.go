package main
import (
    "fmt"
    "net"
    "bufio"
	"time"
	"os"
	"strings"
)

func main() {
	client_id := "1231231"
	client_join := "CLIENT_JOIN|" + client_id + "|0*"
	client_request := "CLIENT_REQUEST|" + client_id + "!10!2*"
    p :=  make([]byte, 2048)
    
	file, err := os.Open("addresses.txt")
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line_components := strings.Split(line, " ")

		conn, err := net.Dial("udp", line_components[0] + ":" + line_components[1])
		if err != nil {
			fmt.Printf("Some error %v", err)
			return
		}
		fmt.Fprintf(conn, client_join)
	}
	file.Close()
	time.Sleep(5 * time.Second)
	conn, err := net.Dial("udp", "127.0.0.1:8000")
	if err != nil {
		fmt.Printf("Some error %v", err)
		return
	}
	fmt.Fprintf(conn, client_request)

    _, err = bufio.NewReader(conn).Read(p)
    if err == nil {
        fmt.Printf("%s\n", p)
    } else {
        fmt.Printf("Some error %v\n", err)
    }
    conn.Close()
}