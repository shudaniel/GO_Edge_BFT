package main
import (
    "fmt"
    "net"
    "bufio"
	"time"
	"os"
	"strings"
	"sync"
	"strconv"
	"encoding/json"
	"io/ioutil"
	"math/rand"

)

var lock_mutex = &sync.Mutex{}

type Latencies struct {
	client_start time.Time
	times []float64
}

type Address struct {
	Zone string
	Ip string
	Port string
}

type Primaries struct {
   string
  Description string
}

func NewLatencyStruct() *Latencies {
	newL := Latencies{
		client_start: time.Now(),
		times: []float64{},
	}

	return &newL
}

func client_thread(client_id string, zone string, num_t int, percent float64,  ch chan *Latencies, summation_ch chan float64, start_signal <-chan bool) {

	client_join := "CLIENT_JOIN|" + client_id + "|" + zone + "*"

	// lock_mutex.Lock()
	file, err := os.Open("addresses.txt")
	if err != nil {
		fmt.Println("Error opening addresses")
		fmt.Println(err)
		return
	}

	l := NewLatencyStruct()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line_component := strings.Split(line, " ")
		conn, err := net.Dial("udp", line_component[0] + ":" + line_component[1])
		if err != nil {
			fmt.Printf("Some error %v", err)
			return
		}

		fmt.Fprintf(conn, client_join)

		conn.Close()
	}
	file.Close()
	// lock_mutex.Unlock()

	var addresses []Address
	file2, _ := ioutil.ReadFile("testing/primaries.json")
	_ = json.Unmarshal([]byte(file2), &addresses)

	// Make a map to use either your zone primmary or primary 0
	directory := make(map[string]net.Conn)

	for j := 0; j < len(addresses); j++ {
		if addresses[j].Zone == zone {
			conn2, err := net.Dial("udp", addresses[j].Ip + ":" + addresses[j].Port)
			if err != nil {
				fmt.Println(err)
				return
			}
			directory["local"] = conn2
		}
		if addresses[j].Zone == "0" {
			conn2, err := net.Dial("udp", addresses[j].Ip + ":" + addresses[j].Port)
			if err != nil {
				fmt.Println(err)
				return
			}
			directory["global"] = conn2
		}
	}


	<-start_signal
	// fmt.Println("Got signal, starting now")

	client_starttime := time.Now()
	for i := 0; i < num_t; i++ {
		p :=  make([]byte, 2048)
		i_str := strconv.Itoa(i)
		client_request := "CLIENT_REQUEST|" + client_id + "!" + i_str + "!10*"
		randnum := rand.Float64()
		// start := time.Now()
		if randnum <= percent {
			fmt.Fprintf(directory["global"], client_request)

			_, err = bufio.NewReader(directory["global"]).Read(p)
			if err == nil {
				// fmt.Printf("%s\n", p)
			} else {
				fmt.Printf("Some error %v\n", err)
			}
			// fmt.Println("Received", string(p), "g")

		} else {
			fmt.Fprintf(directory["local"], client_request)

			_, err = bufio.NewReader(directory["local"]).Read(p)
			if err == nil {
				// fmt.Printf("%s\n", p)
			} else {
				fmt.Printf("Some error %v\n", err)
			}

			// fmt.Println("Received", string(p), "l")
		}
		

		latency_time, _ := strconv.ParseFloat((strings.Split(string( p ), "*")[0]), 64)

		if err == nil && latency_time > 0 {
			// difference := end.Sub(start)
			// total_time := difference.Seconds() 
			lock_mutex.Lock()
			l.times = append(l.times, latency_time)
			lock_mutex.Unlock()
			summation_ch <-latency_time
		} else {
			fmt.Print("Failure on", client_request)
		}
	}

	lock_mutex.Lock()
	l.client_start = client_starttime
	lock_mutex.Unlock()

	ch <- l
}	

func summation(num_t int, ch chan float64, exit chan float64) {
	total := 0.0
	var newval float64
	for i := 0; i < num_t; i++ {	
		newval = <- ch
		total += newval
		fmt.Println(i)
	}
	exit <- total
}

func main() {
	num_c := 10
	num_t := 10
	zone := "0"
	client_id := 0
	percent := 0.5
	ip_addr := "127.0.0.1"
	port := 8000

	argsWithoutProg := os.Args[1:]
	for i, s := range argsWithoutProg {
		switch s {
		case "-i":
			new_i, err := strconv.Atoi(argsWithoutProg[i + 1])
			if err == nil {
				client_id = new_i
			}
		case "-c":
			new_c, err := strconv.Atoi(argsWithoutProg[i + 1])
			if err == nil {
				num_c = new_c
			}
		case "-t":
			new_t, err := strconv.Atoi(argsWithoutProg[i + 1])
			if err == nil {
				num_t = new_t
			}
		
		case "-r":
			new_r, err := strconv.ParseFloat(argsWithoutProg[i + 1], 64)
			if err == nil {
				percent = new_r
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

	p := make([]byte, 2048)
    addr := net.UDPAddr{
        Port: port,
        IP: net.ParseIP(ip_addr),
    }
	ser, err := net.ListenUDP("udp", &addr)
    if err != nil {
        fmt.Printf("Some error %v\n", err)
        return
    }

	summation_ch := make(chan float64)
	final_result_ch := make(chan float64)
	start_signals := make(map[int]chan bool)

	for k := 0; k < num_c; k++ {
		start_signals[k] = make(chan bool)
	}

	_,remoteaddr,err := ser.ReadFromUDP(p)

	go summation(num_t * num_c, summation_ch, final_result_ch)
	ch := make(chan *Latencies)

	for i := 0; i < num_c; i++ {
    	go client_thread( strconv.Itoa(client_id + i), zone, num_t, percent, ch, summation_ch, start_signals[i])
	}

	_,remoteaddr,err = ser.ReadFromUDP(p)

	for h := 0; h < num_c; h++ {
		start_signals[h] <-true
	}
	
	min_time := time.Now()
	for j := 0; j < num_c; j++ {
		latency_result :=  <-ch
		if latency_result.client_start.Before(min_time) {
			min_time = latency_result.client_start
		}
	}

	final_sum := <-final_result_ch

	message := "Total latency:" + strconv.FormatFloat(final_sum, 'f', 6, 64) + "|" + strconv.Itoa(int(min_time.Unix())) + "|" + strconv.Itoa(num_c * num_t) + "*"
	_,err = ser.WriteToUDP([]byte(message), remoteaddr)
    if err != nil {
        fmt.Printf("Couldn't send response %v", err)
    }
	fmt.Println("Done")
}