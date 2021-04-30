package common

const (
	TIMEOUT = 4
)

func HasQuorum(count int, f int) bool {
	return count >= 2*f + 1
}