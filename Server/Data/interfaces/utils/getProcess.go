package utils

import (
	"net"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/balancer"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/providers"
)

func GetAllProcessByClientSocketConn(clientSocket net.Conn) ([]*providers.ProcessEvent, []*providers.ProcessState) {
	var events []*providers.ProcessEvent
	var states []*providers.ProcessState

	for _, task := range balancer.BalancerEventQueue.GetTasks() {
		if task.ClientSocket.Conn == clientSocket {
			events = append(events, task)
		}
	}

	for _, task := range balancer.BalancerErrorEventQueue.GetTasks() {
		if task.ClientSocket.Conn == clientSocket {
			events = append(events, task)
		}
	}

	for _, task := range balancer.BalancerStatesQueue.GetTasks() {
		if task.ClientSocket.Conn == clientSocket {
			states = append(states, task)
		}
	}

	for _, task := range balancer.BalancerErrorStatesQueue.GetTasks() {
		if task.ClientSocket.Conn == clientSocket {
			states = append(states, task)
		}
	}

	return events, states
}
