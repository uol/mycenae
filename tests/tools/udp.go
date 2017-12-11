package tools

import (
	"log"
	"net"
)

type udpTool struct {
	conn *net.UDPConn
	addr *net.UDPAddr
}

func (uTool *udpTool) Init(hostname string, port string) {
	var err error

	uTool.conn, err = net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		log.Println("UDP conn:", err)
	}

	uTool.addr, err = net.ResolveUDPAddr("udp", hostname+":"+port)
	if err != nil {
		log.Println("UDP address:", err)
	}

	return
}

func (uTool *udpTool) SendString(payload string) (err error) {
	return uTool.Send([]byte(payload))
}

func (uTool *udpTool) Send(pack []byte) (err error) {
	if _, err = uTool.conn.WriteToUDP(pack, uTool.addr); err != nil {
		log.Println("UDP write:", err)
	}
	return
}
