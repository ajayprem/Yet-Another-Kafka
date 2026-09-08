package types

import (
	"net"
	"strconv"
)

func localIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String(), nil
}

func GetLocalAddress(port int) (string, error) {
	ip, err := localIP()
	if err != nil {
		return "", err
	}

	return ip + ":" + strconv.Itoa(port), nil
}
