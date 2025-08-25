package handler_connections

import (
	"fmt"
	"log"
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
			fmt.Println("Tipo de dato no reconocido:", res)
		}
	})
}
func HandleEventDispatcher(result comunication.Event, ClientSocket *client.ClientSocket, clients *client.ClientSliceGroups) {

	if result.Destination == "127.0.0.1" {
		// Manejar el estado para el cliente local
		fmt.Println("Evento recibido:", result.Event, "de", result.Origen)
	} else {
		clientDestination, err := clients.SearchClientByNameGetClient(strings.ToUpper(result.Destination))

		if err != nil {
			log.Println("Error al buscar el cliente destino:", err)
			var state comunication.State = comunication.State{Status: false, Message: "Error: Cliente no encontrado", Error: "Client not found", Data: nil}
			client.Emit(state, ClientSocket)
			return
		}

		var process providers.Process = providers.Process{PIDC: result.PID, TTL: 60, Attempts: 3, Created: time.Now(), Updated: time.Now(), DataSend: result, ClientSocket: clientDestination}
		balancer.BalancerQueue.AddTask(&process)
	}

}

func HandleStateDispatcher(result comunication.State, clientSocket *client.ClientSocket, clients *client.ClientSliceGroups) {
	if result.Destination == "127.0.0.1" {

		fmt.Println("Estado recibido:", result.Message, "de", result.Origen)

	} else {
		clientDestination, err := clients.SearchClientByNameGetClient(strings.ToUpper(result.Destination))
		println("Estado recibido:", result.Message, "de", result.Origen, "a", result.Destination)
		if err != nil {
			log.Println("Error al buscar el cliente destino:", err)
			var state comunication.State = comunication.State{Status: false, Message: "Error: Cliente no encontrado", Error: "Client not found", Data: nil}
			client.Emit(state, clientSocket)
			return
		}

		client.Emit(result, clientDestination)
	}
}
