package client_socket

import (
	"net"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/request_response"
)

type ClientSocket struct {
	Host       string
	Port       string
	Conn       net.Conn
	NameClient string
	Events     chan request_response.Event
	States     chan request_response.State
}
