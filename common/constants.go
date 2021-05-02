package common

// import (
// 	"time"
// )

const (
	TIMEOUT = 4
	MAJORITY = 2
	MAX_CHANNEL_SIZE = 5000
	MESSAGE_ENDER = "~"
	MESSAGE_DELIMITER = "*"
	VERBOSE = false   // Only print received messages, sent messages, and timeouts
	VERBOSE_EXTRA = false // Print additional information. VERBOSE must also be true

)

func HasQuorum(count int, f int) bool {
	return count >= (2*f) + 1  // Subtract one from 2f + 1 because you can count that you already voted for yourself
}

// func Timeout(duration int, ch chan bool) {
// 	time.Sleep(duration * time.Second)
// 	ch <- false
// }