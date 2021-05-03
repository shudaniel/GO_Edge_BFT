package main
import (
	"EdgeBFT/common"
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
	"regexp"

)

var lock_mutex = &sync.Mutex{}

type Latencies struct {
	client_start int
	client_end int
	duration float64
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

func handleConnection(c net.Conn, results chan Latencies, signal chan bool) {

	parseMessage := func(input chan string, result chan Latencies, signal chan bool ) {
		var isValidString = regexp.MustCompile(`^[a-zA-Z0-9.,|_~]*$`).MatchString 
		message := ""
		for {
			received_data := <- input
			for _, value := range strings.Split(strings.TrimSpace(received_data), common.MESSAGE_DELIMITER) {
				if len(value) > 0 && isValidString(value) {
					// Check if the end of the message is "end." Otherwise this is a partial message and you must wait for the rest
					if value[len(value)-1:] == common.MESSAGE_ENDER {
						
						temp :=strings.Split(value, "|") [0]
						complete_message := strings.Split(message + temp, ",")
						duration, err1 := strconv.ParseFloat(complete_message[0], 64)
						start, err2 := strconv.Atoi(complete_message[1])
						end, err3 := strconv.Atoi(complete_message[2])
						if err1 != nil || err2 != nil || err3 != nil {
							fmt.Println(err1)
							fmt.Println(err2)
							fmt.Println(err3)
						} else {
							// fmt.Println(temp2)
							result <- Latencies {
								client_start: start,
								client_end: end,
								duration: duration,
							}
							signal <- true
						}
						message = ""
					} else {
						message = message + value
					}
					
				}
			}
			
			

		}
	}

	input := make(chan string, 10000)

	
	go parseMessage(input, results, signal)

	for {
		p := make([]byte, 128)
		_, err := c.Read(p)
		if err == nil {
			input <- string(p)
		}
		// temp := (strings.Split(string( p ), "*"))[1]
		// fmt.Println("Temp:", temp)
		// latency_time, _ := strconv.ParseFloat( strings.Split(temp, "|")[0], 64)

	}
}

func client_thread(client_id string, zone string, num_t int, percent float64, summation_ch chan Latencies, start_signal <-chan bool) {

	

	var addresses []Address
	file2, _ := ioutil.ReadFile("testing/primaries.json")
	_ = json.Unmarshal([]byte(file2), &addresses)

	// Make a map to use either your zone primmary or primary 0
	directory := make(map[string]net.Conn)
	signal := make(chan bool)
	for j := 0; j < len(addresses); j++ {
		if addresses[j].Zone == zone {
			conn2, err := net.Dial("tcp", addresses[j].Ip + ":" + addresses[j].Port)
			if err != nil {
				fmt.Println(err)
				return
			}
			directory["local"] = conn2

			p := make([]byte, 1024)
			_, err = conn2.Read(p)
			// fmt.Println("Received:", string(p))

			go handleConnection(conn2, summation_ch, signal)
		}
		if addresses[j].Zone == "0" {
			conn2, err := net.Dial("tcp", addresses[j].Ip + ":" + addresses[j].Port)
			if err != nil {
				fmt.Println(err)
				return
			}
			directory["global"] = conn2
			p := make([]byte, 1024)
			_, err = conn2.Read(p)
			// fmt.Println("Received:", string(p))

			go handleConnection(conn2, summation_ch, signal)
		}
	}

	
	// Read the start signal
	<-start_signal
	// fmt.Println("Got signal, starting now")

	// client_starttime := time.Now()
	for i := 0; i < num_t; i++ {
		// p :=  make([]byte, 512)
		i_str := strconv.Itoa(i)
		client_request := common.MESSAGE_DELIMITER + "CLIENT_REQUEST|" + client_id + "!" + i_str + "!10|" + common.MESSAGE_ENDER + common.MESSAGE_DELIMITER
		randnum := rand.Float64()
		// start := time.Now()
		if randnum < percent {
			directory["global"].Write([]byte(client_request))
			// fmt.Fprintf(directory["global"], client_request)

			// _, err = bufio.NewReader(directory["global"]).Read(p)
			// if err == nil {
			// 	// fmt.Printf("%s\n", p)
			// } else {
			// 	fmt.Printf("Some error %v\n", err)
			// }
			// fmt.Println("Received", string(p), "g")

		} else {
			directory["local"].Write([]byte(client_request))
			// fmt.Fprintf(directory["local"], client_request)

			// _, err = bufio.NewReader(directory["local"]).Read(p)
			// if err == nil {
			// 	// fmt.Printf("%s\n", p)
			// } else {
			// 	fmt.Printf("Some error %v\n", err)
			// }

			// fmt.Println("Received", string(p), "l")
		}
		<-signal
		
	// 	temp := (strings.Split(string( p ), "*"))[1]
	// 	fmt.Println("Temp:", temp)
	// 	latency_time, _ := strconv.ParseFloat( strings.Split(temp, "|")[0], 64)

	// 	if err == nil && latency_time > 0 {
	// 		// difference := end.Sub(start)
	// 		// total_time := difference.Seconds() 
	// 		lock_mutex.Lock()
	// 		l.times = append(l.times, latency_time)
	// 		lock_mutex.Unlock()
	// 		summation_ch <-latency_time
	// 	} else {
	// 		fmt.Println("Failure on", client_request, latency_time)
	// 	}
	}

	// lock_mutex.Lock()
	// l.client_start = client_starttime
	// lock_mutex.Unlock()

	// ch <- l

	directory["local"].Close()
	directory["global"].Close()
}	

type FinalResult struct {
	total_latencies  float64
	num_successes  int
	earliest int
	latest int
}

func summation(num_t int, ch chan Latencies, exit chan FinalResult) {
	total := 0.0
	earliest := 0
	latest := 0
	num_successes := 0
	var newval Latencies
	for i := 0; i < num_t; i++ {	
		newval = <- ch
		total += newval.duration

		// fmt.Println(i)
		if newval.duration > 0 {
			num_successes++

			if earliest == 0 || newval.client_start < earliest {
			earliest = newval.client_start
			}
			if latest == 0 || newval.client_end > latest {
				latest = newval.client_start
			}

		}

	}
	exit <- FinalResult {
		total_latencies: total,
		num_successes: num_successes,
		earliest: earliest,
		latest: latest,
	}
}

func main() {

	rand.Seed(10)
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

	summation_ch := make(chan Latencies)
	final_result_ch := make(chan FinalResult)
	start_signals := make(map[int]chan bool)

	for k := 0; k < num_c; k++ {
		start_signals[k] = make(chan bool)
	}

	_,remoteaddr,err := ser.ReadFromUDP(p)


	client_join := common.MESSAGE_DELIMITER + "CLIENT_JOIN|" + strconv.Itoa(client_id) + "|" + zone + "|" + strconv.Itoa(num_c) + "|" + common.MESSAGE_ENDER + common.MESSAGE_ENDER

	// lock_mutex.Lock()
	file, err := os.Open("addresses.txt")
	if err != nil {
		fmt.Println("Error opening addresses")
		fmt.Println(err)
		return
	}

	// l := NewLatencyStruct()
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


	go summation(num_t * num_c, summation_ch, final_result_ch)
	// ch := make(chan *Latencies)

	for i := 0; i < num_c; i++ {
    	go client_thread( strconv.Itoa(client_id + i), zone, num_t, percent, summation_ch, start_signals[i])
		time.Sleep(20 * time.Millisecond)
	}

	_,remoteaddr,err = ser.ReadFromUDP(p)

	for h := 0; h < num_c; h++ {
		start_signals[h] <-true
	}
	
	// min_time := time.Now()
	// for j := 0; j < num_c; j++ {
	// 	latency_result :=  <-ch
	// 	if latency_result.client_start.Before(min_time) {
	// 		min_time = latency_result.client_start
	// 	}
	// }

	final_sum := <-final_result_ch

	message := "Total latency:" + strconv.FormatFloat(final_sum.total_latencies, 'f', 6, 64) + "|" + strconv.Itoa(final_sum.earliest) + "|" + strconv.Itoa(final_sum.latest) + "|" + strconv.Itoa(final_sum.num_successes) + "*"
	_,err = ser.WriteToUDP([]byte(message), remoteaddr)
    if err != nil {
        fmt.Printf("Couldn't send response %v", err)
    }
	fmt.Println("Done")
}