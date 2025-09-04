package handler_connections

import (
	"fmt"
	"strings"
	"time"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/balancer"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/providers"
)

func HandleConnection(client *client.ClientSocket, clients *client.ClientSliceGroups) {

	defer client.Conn.Close()
	connected := make(chan bool, 1)
	connected <- true
	println("Cliente conectado: ", client.Host)
	go HandleConnectionDispatcher(client, clients)
	go client.ReadData(connected)

	select {
	case <-connected:
		if !<-connected {
			fmt.Println("Cliente desconectado: ", client.Info.ClientName)
			clients.RemoveClient(client.Host)
			fmt.Println("Cerrando conexion")
			return
		}

	default:
		fmt.Println("Cliente desconectado")
	}

}

func HandleConnectionDispatcher(client *client.ClientSocket, clients *client.ClientSliceGroups) {
	client.On(func(result interface{}) {
		switch res := result.(type) {
		case comunication.Event:
			go HandleEventDispatcher(res, client, clients)
		case comunication.State:
			go HandleStateDispatcher(res, client, clients)
		default:
			//fmt.Println("Tipo de dato no reconocido:", res)
		}
	})
}
func HandleEventDispatcher(result comunication.Event, ClientSocket *client.ClientSocket, clients *client.ClientSliceGroups) {

	clientDestination, err := clients.SearchClientByNameGetClient(strings.ToUpper(result.Destination), 0)
	var process providers.ProcessEvent = providers.ProcessEvent{PIDC: result.PID, TTL: 60, Attempts: 3, Created: time.Now(), Updated: time.Now(), DataSend: result, ClientSocket: clientDestination, ClientPos: 0}

	if err != nil {
		var state comunication.State = comunication.State{Status: false, Message: "Error: Cliente no encontrado", Error: "Client not found", Data: nil}
		client.Emit(state, ClientSocket)
		return
	}

	balancer.BalancerEventQueue.AddTask(&process)

}

func HandleStateDispatcher(result comunication.State, clientSocket *client.ClientSocket, clients *client.ClientSliceGroups) {
	switch result.Destination {
	case "127.0.0.1-S":

		var errorProc bool = false

		currentProcess := balancer.BalancerStatesQueue.GetTaskByPID(result.SERVERPID)

		if currentProcess == nil {
			currentProcess = balancer.BalancerErrorStatesQueue.GetTaskByPID(result.SERVERPID)
			errorProc = true
		}

		if currentProcess != nil {

			if !result.Status {

				balancer.BalancerErrorStatesQueue.AddTask(currentProcess)
			}

			if errorProc {
				balancer.BalancerErrorStatesQueue.RemoveTaskByPID(result.SERVERPID)
			} else {
				balancer.BalancerStatesQueue.RemoveTaskByPID(result.SERVERPID)
			}
		}

	case "127.0.0.1-E":

		var errorProc bool = false

		currentProcess := balancer.BalancerEventQueue.GetTaskByPID(result.SERVERPID)

		if currentProcess == nil {
			currentProcess = balancer.BalancerErrorEventQueue.GetTaskByPID(result.SERVERPID)
			errorProc = true
		}

		if currentProcess != nil {
			if !result.Status {
				balancer.BalancerErrorEventQueue.AddTask(currentProcess)
			}

			if errorProc {
				balancer.BalancerErrorEventQueue.RemoveTaskByPID(result.SERVERPID)
			} else {
				balancer.BalancerEventQueue.RemoveTaskByPID(result.SERVERPID)
			}
		}

	default:
		currentProcess := balancer.BalancerEventQueue.GetTaskByPID(result.SERVERPID)
		var pidc int = 0
		if currentProcess != nil {
			pidc = currentProcess.PIDC
		}

		result.LOCALPID = pidc
		clientDestination, err := clients.SearchClientByNameGetClient(strings.ToUpper(result.Destination), 0)
		var process providers.ProcessState = providers.ProcessState{PIDC: pidc, PID: result.SERVERPID, TTL: 60, Attempts: 3, Created: time.Now(), Updated: time.Now(), DataSend: result, ClientSocket: clientDestination, ClientPos: 0}
		if err != nil {
			var state comunication.State = comunication.State{Status: false, Message: "Error: Cliente no encontrado", Error: "Client not found", Data: nil}
			client.Emit(state, clientSocket)
			return
		}

		balancer.BalancerStatesQueue.AddTask(&process)
	}

}
