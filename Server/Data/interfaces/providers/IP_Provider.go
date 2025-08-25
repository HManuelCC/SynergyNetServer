package providers

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type IPProvider struct {
	IP    string
	InUse bool
}

func GenerateVirtualIp() (string, error) {
	rand.Seed(time.Now().UnixNano())
	ip := fmt.Sprintf("192.168.%d.%d", rand.Intn(256), rand.Intn(256))
	if !isValidIp(ip) {
		return "", fmt.Errorf("invalid IP address generated: %s", ip)
	}
	return ip, nil

}

func isValidIp(ip string) bool {
	parts := strings.Split(ip, ".")
	if len(parts) != 4 {
		return false
	}
	for _, part := range parts {
		num, err := strconv.Atoi(part)
		if err != nil || num < 0 || num > 255 {
			return false
		}
	}
	return true
}

func isUniqueIp(ip string, existingIps []string) bool {
	for _, existingIp := range existingIps {
		if existingIp == ip {
			return false
		}
	}
	return true
}
