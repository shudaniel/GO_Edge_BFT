package common

const (
	TIMEOUT = 4
)

func HasQuorum(count int, f int) bool {
	return count >= (2*f)  // Subtract one from 2f + 1 because you can count that you already voted for yourself
}