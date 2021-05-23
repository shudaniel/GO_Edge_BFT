package lib 

import (
  "fmt"
  "net"
  "os"
  "time"
  "bufio"
  "encoding/json"
)

////////////////////////////// Variables ///////////////////////////

type fn func(map[string]interface{})

/* Maintain the connections once created to reduce number of
connections. */
var connections map[string]net.Conn
var V bool /* V=true: Verbose. V=false: Non-Verbose */ 

////////////////////////////// Functions ///////////////////////////

func SendRequest(service string, msg []byte, handleRequestType fn) {
  /* Send msg to ip:port sent in service. */
  if V { fmt.Println("Sent Message: " + string(msg) + "\n") }
  var ok bool
  var conn net.Conn
  if (connections == nil) {
    connections = make(map[string]net.Conn)
  }
  if conn, ok = connections[service]; !ok {
    tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
    if CheckConnError(err, conn) { return }
    conn, err = net.DialTCP("tcp", nil, tcpAddr)
    if CheckConnError(err, conn) { return }
    connections[service] = conn
    go connHandler(conn, handleRequestType)
  }
  msg = append(msg, "\n"...)
  if conn != nil {
    err := conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
    if CheckConnError(err, conn) { return }
    _, err = conn.Write(msg)
    if CheckConnError(err, conn) { return }
  }
}

func connHandler(conn net.Conn, handleRequestType fn) {
  ch := make(chan []byte)
  eCh := make(chan error)
  reader := bufio.NewReader(conn)
  // Start a goroutine to read from our net connection
  go func(ch chan []byte, eCh chan error) {
    for {
      // try to read the data
      data, err := reader.ReadString('\n')
      if err != nil {
        // send an error if it's encountered
        eCh<- err
      } else {
        // send data if we read some.
        ch<- []byte(data)
      }
    }
  }(ch, eCh)

  // continuously read from the connection
  for {
    select {
     // This case means we recieved data on the connection
     case data := <-ch:
      request:= LoadRequest(data)
      handleRequestType(request)
     // This case means we got an error and the goroutine has finished
     case err := <-eCh:
       // handle our error then exit for loop
       fmt.Println(err)
       HandleConnClosed(conn)
       return
    }
  }
}


func HandleConnClosed(c net.Conn) {
  for k, v  := range connections {
    if v == c {
      delete(connections, k)
    }
  }
}

func CheckConnError(err error, conn net.Conn) bool {
  if (err != nil) {
    HandleConnClosed(conn)
    return true
  }
  return false
}

func CheckError(err error) {
  if err != nil {
    fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
    // os.Exit(1)
  }
}

func LoadRequest(requestBytes []byte) map[string]interface{} {
  /* Convert JSON format of incoming request to a map */
  if V { fmt.Println("Received Message: " + string(requestBytes)) }
  var request map[string]interface{}
  err := json.Unmarshal(requestBytes, &request)
  CheckError(err)
  return request
}