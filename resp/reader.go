package resp

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

func ParseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

	if line[0] != '*' {
		return nil, fmt.Errorf("expected array, got %s", line)
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array count: %s", line[1:])
	}
	args := make([]string, 0, count)

	for range count {
		// Read the $N line
		_, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		// Read the actual value
		value, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		args = append(args, strings.TrimSpace(value))
	}

	return args, nil
}
