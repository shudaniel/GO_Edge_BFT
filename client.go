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
	"regexp"
	"math"
	"github.com/libp2p/go-reuseport"
)

// var all_start bool
var lock_mutex = &sync.Mutex{}

type Latencies struct {
	start string
	end string
	time float64
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

func handleConnection(c net.Conn, result chan Latencies, signal chan bool) {

	// parseMessage := func(input chan string, result chan Latencies, signal chan bool) {
	// 	var isValidString = regexp.MustCompile(`^[a-zA-Z0-9_:!|.;,~/]*$`) 
	// 	for {
	// 		raw_msg := <-input
	// 		// value = strings.Split(strings.TrimSpace(value), common.MESSAGE_DELIMITER)[1]
	// 		matches := isValidString.FindAllString(raw_msg, -1) 
	// 		for _, value := range matches {
	// 			if len(value) > 0{
	// 				// fmt.Println("received", value)
	// 				signal <-true
			
	// 				temp :=strings.Split(value, "|") [0]
	// 				components := strings.Split(temp, ",")
	// 				temp2, err := strconv.ParseFloat(components[0], 64)
	// 				if err != nil {
	// 					fmt.Println(err)
	// 				} else {
	// 					// fmt.Println(temp2)
	// 					result <- Latencies {
	// 						time: temp2,
	// 						start: components[1],
	// 						end: components[2],
	// 					}
					
	// 				}
	// 			}
	// 		}	
			
	// 	}
	// }

	// handleMessage := func(input chan string, output chan string, signal chan bool ) {
	// 	// var isValidString = regexp.MustCompile(`^[a-zA-Z0-9.|_~]*$`).MatchString 
	// 	// message := ""
	// 	for {
	// 		received_data := <- input
	// 		output <- strings.Split(strings.TrimSpace(received_data), common.MESSAGE_DELIMITER)[1]
	// 		// fmt.Println("RAW RECEIVED:", received_data)
	// 		// for _, value := range strings.Split(strings.TrimSpace(received_data), common.MESSAGE_DELIMITER) {
	// 		// 	fmt.Println("parse received:", value)
	// 		// 	if len(value) > 0 && isValidString(value) {
	// 		// 		// Check if the end of the message is "end." Otherwise this is a partial message and you must wait for the rest
	// 		// 		if value[len(value)-1:] == common.MESSAGE_ENDER {
						
	// 		// 			output <- message + value
	// 		// 			message = ""
	// 		// 		} else {
	// 		// 			message = message + value
	// 		// 		}
					
	// 		// 	}
	// 		// }
			
			

	// 	}
	// }

	// input := make(chan string, 1000)
	// tunnel := make(chan string, 1000)

	
	// go handleMessage(input, tunnel, signal)
	// go parseMessage(input, result, signal)

	for {
		p := make([]byte, 55)
		_, err := c.Read(p)
		if err == nil {
			signal <-true
			// input <- string(p)
		}
		// temp := (strings.Split(string( p ), "*"))[1]
		// fmt.Println("Temp:", temp)
		// latency_time, _ := strconv.ParseFloat( strings.Split(temp, "|")[0], 64)

	}
}

func client_thread(client_id string, zone string, num_t int, txns []string, summation_ch chan Latencies, start_signal <-chan bool, done_ch chan<- bool) {

	

	var addresses []Address
	file2, _ := ioutil.ReadFile("testing/primaries.json")
	_ = json.Unmarshal([]byte(file2), &addresses)

	// Make a map to use either your zone primmary or primary 0
	directory := make(map[string]net.Conn)
	// signal := make(chan bool)
	for j := 0; j < len(addresses); j++ {
		// if addresses[j].Zone == zone {
		// 	conn2, err := net.Dial("tcp", addresses[j].Ip + ":" + addresses[j].Port)
		// 	if err != nil {
		// 		fmt.Println(err)
		// 		return
		// 	}
		// 	directory["local"] = conn2

		// 	p := make([]byte, 1024)
		// 	_, err = conn2.Read(p)
		// 	// fmt.Println("Received:", string(p))

		// 	go handleConnection(conn2, summation_ch, signal)
		// } else {
		conn2, err := net.Dial("tcp", addresses[j].Ip + ":" + addresses[j].Port)
		if err != nil {
			fmt.Println(err)
			return
		}
		directory[ addresses[j].Zone ] = conn2
		p := make([]byte, 1024)
		_, err = conn2.Read(p)
		// fmt.Println("Received:", string(p))

		// go handleConnection(conn2, summation_ch, signal)
		// }
	}

	
	// Read the start signal
	<-start_signal
	// for all_start {}
	// fmt.Println("Got signal, starting now")

	if num_t != len(txns) {
		fmt.Println("num_t and txns not the same", num_t, txns)
	}

	// client_starttime := time.Now()
	previous_zone := "-1"
	p := make([]byte, 1024)
	for i := 0; i < num_t; i++ {
		// p :=  make([]byte, 512)
		i_str := strconv.Itoa(i)
		txn_type := txns[i][0:1]
		client_request := common.MESSAGE_DELIMITER + "CLIENT_REQUEST|" + client_id + "!" + i_str + "!10" 
		// start := time.Now()

		// fmt.Println("Starting :" + client_id + "!" + i_str + "!10")
		// start := time.Now())
		if txn_type == "l" {
			client_request += "!l|" + common.MESSAGE_ENDER + common.MESSAGE_DELIMITER
			// directory["local"].Write([]byte(client_request))
			directory[zone].Write([]byte(client_request))
			// fmt.Fprintf(directory["local"], client_request)

			// _, err = bufio.NewReader(directory["local"]).Read(p)
			// if err == nil {
			// 	// fmt.Printf("%s\n", p)
			// } else {
			// 	fmt.Printf("Some error %v\n", err)
			// }

			// fmt.Println("Received", string(p), "l")
			previous_zone = zone
		} else {
			global_zone := txn_type
			if previous_zone != global_zone {
				// A leader election is needed
				client_request += "!G"
			} else {
				client_request += "!g"
			}
			client_request += "|" + common.MESSAGE_ENDER + common.MESSAGE_DELIMITER

			zone = global_zone
			directory[global_zone].Write([]byte(client_request))
			// fmt.Fprintf(directory["global"], client_request)

			// _, err = bufio.NewReader(directory["global"]).Read(p)
			// if err == nil {
			// 	// fmt.Printf("%s\n", p)
			// } else {
			// 	fmt.Printf("Some error %v\n", err)
			// }
			// fmt.Println("Received", string(p), "g")
			previous_zone = global_zone

		} 
		directory[zone].Read(p)
		// fmt.Println("Done:", string(p))
		// <-signal
		// fmt.Println("Signal received to start next", client_id)
		
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

	for _, v := range directory {
		v.Close()
	}
	// directory["local"].Close()
	

	done_ch <- true
}	

type FinalResult struct {
	total_latencies  float64
	num_successes  int
	earliest int
	latest int
}

func summation(num_t int, ch chan Latencies, exit chan FinalResult) {
	total := 0.0
	num_successes := 0
	earliest := 0
	latest := 0
	var nums[1000000] float64
	var newval Latencies
	for i := 0; i < num_t; i++ {	
		newval = <- ch
		
		// fmt.Println(i)
		if newval.time > 0 {
			total += newval.time
			num_successes++
			val, _ := strconv.Atoi(newval.start)
			if earliest == 0 || val < earliest {
				earliest = val
			}
			val, _ = strconv.Atoi(newval.end)
			if val > latest {
				latest = val
			}
		}
		nums[i] = newval.time
	}
	// Calculate standard deviation
	var sd float64
	mean := total / float64(num_successes)
	for j := 0; j < num_t; j++ {
		if nums[j] > 0 {
			sd += math.Pow( nums[j] - mean, 2 )
		}
	}
	sd = math.Sqrt(sd / float64(num_successes))
	fmt.Println("Mean:", mean)
	fmt.Println("StdDev:", sd)
	exit <- FinalResult {
		total_latencies: total,
		num_successes: num_successes,
		earliest: earliest,
		latest: latest,
	}
}

func main() {

	num_c := 10
	num_t := 10
	zone := "0"
	client_id_seed := 0
	// percent := 0.5
	ip_addr := "127.0.0.1"
	port := 8000

	argsWithoutProg := os.Args[1:]
	for i, s := range argsWithoutProg {
		switch s {
		
		case "-a":
			ip_addr = argsWithoutProg[i + 1]
		
		case "-p":
			new_p, err := strconv.Atoi(argsWithoutProg[i + 1])
			if err == nil {
				port = new_p
			}
		}

	}


	p := make([]byte, 2049)
    addr := net.UDPAddr{
        Port: port,
        IP: net.ParseIP(ip_addr),
    }
	serudp, err := net.ListenUDP("udp", &addr)
    if err != nil {
        fmt.Printf("Some error %v\n", err)
        return
    }

	ser, err := reuseport.Listen("tcp", ip_addr + ":" + strconv.Itoa(port))
    if err != nil {
        fmt.Printf("Some error %v\n", err)
        return 
    }

	c, err := ser.Accept()

	// var remoteaddr *net.UDPAddr
	done_ch := make(chan bool, num_c)
	summation_ch := make(chan Latencies, num_c * num_t)
	final_result_ch := make(chan FinalResult)
	start_signals := make(map[int]chan bool)

	waiting_for_start_signal := true
	start_message := ""
	re := regexp.MustCompile(`[a-zA-Z0-9_:!|.;,~/{}"\[\] ]*`) 
	for waiting_for_start_signal {
		_, err := c.Read(p)
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("EOF detected")
				break
			}
		}
		// fmt.Println("Fragment", string(p))
		for _, value := range strings.Split(strings.TrimSpace(string(p)), common.MESSAGE_DELIMITER) {
			// fmt.Println("Fragment", len(value), value)
			matches := re.FindAllString(value, -1)
			for _, v := range matches {
				if len(v) > 0 {
					start_message = start_message + v
					fmt.Println(len(start_message))
					// Check if the end of the message is "end." Otherwise this is a partial message and you must wait for the rest
					if start_message[len(start_message)-1:] == common.MESSAGE_ENDER {
						waiting_for_start_signal = false	
						break
					} 
					
				} 
			}
			
		}

	}
	c.Close()
	start_message = start_message[0:len(start_message)-1]
	// fmt.Println("Start message:", start_message)

	fmt.Println("First signal received")
	params := strings.Split( start_message, "|" )

	zone = params[0]
	num_c, err = strconv.Atoi(params[1])
	if err != nil {
		fmt.Println(err)
	}
	num_t, err = strconv.Atoi(params[2])
	if err != nil {
		fmt.Println(err)
	}
	// percent, _ = strconv.ParseFloat(params[3], 64)

	client_txn_data := make(map[string]string)
	json_err := json.Unmarshal([]byte(params[4]), &client_txn_data)
	if json_err != nil {
		fmt.Println(json_err)
	}

	baseline, err := strconv.Atoi(params[5])

	fmt.Println("NUM_T:", num_t, ", NUM_C:", num_c, "zone:", zone, "baseline", baseline)

	for k := 0; k < num_c; k++ {
		start_signals[k] = make(chan bool)
	}


	zone_num, err := strconv.Atoi(zone)
	if err != nil {
		fmt.Println(err)
	}
	client_id_seed = zone_num * num_c
	if baseline == 1 {
		zone = "0"
	}
	fmt.Println("Client id seed", client_id_seed)
	client_join := common.MESSAGE_DELIMITER + "CLIENT_JOIN|" + strconv.Itoa(client_id_seed) + "|" + zone + "|" + strconv.Itoa(num_c) + "|" + common.MESSAGE_ENDER + common.MESSAGE_DELIMITER

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

	// If baseline is set to 1, then that means everyone is doing pbft and their zone should be 0

	for i := 0; i < num_c; i++ {
		client_id := strconv.Itoa(client_id_seed + i) 
    	go client_thread( client_id, zone, num_t, strings.Split(client_txn_data[client_id], ","), summation_ch, start_signals[i], done_ch)
		time.Sleep(20 * time.Millisecond)
	}

	_,_,_ = serudp.ReadFromUDP(p)
	fmt.Println("Second signal received")

	for h := 0; h < num_c; h++ {
		start_signals[h] <-true
	}
	// all_start = false

	for h := 0; h < num_c; h++ {
		<-done_ch
	}
	
	// min_time := time.Now()
	// for j := 0; j < num_c; j++ {
	// 	latency_result :=  <-ch
	// 	if latency_result.client_start.Before(min_time) {
	// 		min_time = latency_result.client_start
	// 	}
	// }

	// final_sum := <-final_result_ch
	// fmt.Println("Latency:", final_sum.total_latencies)
	// fmt.Println("Num:", final_sum.num_successes)

	// message := strconv.FormatFloat(final_sum.total_latencies, 'f', 6, 64) + "|" + strconv.Itoa(final_sum.earliest) + "|" + strconv.Itoa(final_sum.latest) + "|" + strconv.Itoa(final_sum.num_successes) + "*"
	// _,err = ser.WriteToUDP([]byte(message), remoteaddr)
    // if err != nil {
    //     fmt.Printf("Couldn't send response %v", err)
    // }
	fmt.Println("Done")


}