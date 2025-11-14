package handler_connections

import (
	"log"
	"time"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/balancer"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/providers"
)

var max int = 0 // Variable global para almacenar el valor máximo

func HandleConnection(client *client.ClientSocket, clients *client.ClientSliceGroups, clientsEventSubs *client.ClientSliceGroupMapEventSubscription) {

	defer client.Conn.Close()
	connected := make(chan bool, 1)
	connected <- true
	println("Cliente conectado: ", client.Host)
	go HandleConnectionDispatcher(client, clients)
	go client.ReadData(connected)

	select {
	case <-connected:
		if !<-connected {

			log.Println("Cliente desconectado: ", client.Info.ClientName)

			clients.RemoveClient(client.Host)
			clientsEventSubs.RemoveSubscriber(client)

			log.Println("Cerrando conexion")

			return
		}

	default:

		log.Println("Cliente desconectado")

	}

}

func HandleConnectionDispatcher(client *client.ClientSocket, clients *client.ClientSliceGroups) {
	client.On(func(result interface{}) {
		switch res := result.(type) {
		case comunication.Event:
			go HandleEventDispatcher(res, client, clients)
		case comunication.State:
			go HandleStateDispatcher(res, client, clients)
		case comunication.MessageState:
			go HandleClientMessageState(res, client, clients)
		default:
			log.Printf("Unrecognized data type received from client %s: %T", client.Info.ClientName, res)
		}
	})
}
func HandleEventDispatcher(result comunication.Event, ClientSocket *client.ClientSocket, clients *client.ClientSliceGroups) {
	result.Origen = ClientSocket.Info.ClientName

	var process providers.Process = providers.Process{
		PID:          0,
		TTL:          5,
		Attempts:     3,
		Created:      time.Now(),
		Updated:      time.Now(),
		DataSend:     result,
		ClientSocket: nil,
		ClientPos:    0,
		Type:         "EVENT",
		ClientPID:    result.PID,
	}

	balancer.BalancerQueue.AddTask(process)
}

func HandleStateDispatcher(result comunication.State, clientSocket *client.ClientSocket, clients *client.ClientSliceGroups) {
	var processEvent = balancer.BalancerQueue.GetTaskByPID(result.PID)
	if processEvent == nil {
		return
	}
	var clientPid = 0
	balancer.BalancerQueue.Mutex.Lock()
	clientPid = processEvent.ClientPID
	processEvent.Updated = time.Now()
	balancer.BalancerQueue.RemoveTaskByPID(result.PID)
	balancer.BalancerQueue.Mutex.Unlock()

	result.PID = clientPid
	result.Origen = clientSocket.Info.ClientName

	var process providers.Process = providers.Process{
		PID:          0,
		TTL:          5,
		Attempts:     3,
		Created:      time.Now(),
		Updated:      time.Now(),
		DataSend:     result,
		ClientSocket: nil,
		ClientPos:    0,
		Type:         "STATE",
	}

	balancer.BalancerQueue.AddTask(process)
}

func HandleClientMessageState(result comunication.MessageState, clientSocket *client.ClientSocket, clients *client.ClientSliceGroups) {

	//fmt.Println("Processing MessageState from client:", clientSocket.Info.ClientName, "MessageState:", result.ToString())
	err := balancer.BalancerQueue.ManageProcessWithMessageState(result, clientSocket)
	if err != nil {
		// Log del error y el mensaje que causó el problema
		log.Printf("Error processing MessageState from client %s: %v. MessageState: %s",
			clientSocket.Info.ClientName, err, result.ToString())
		return
	}

	//balancer.BalancerQueue.Print()

}
