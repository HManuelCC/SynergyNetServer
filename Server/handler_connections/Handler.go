package handler_connections

import (
	"fmt"

	client_socket "github.com/HManuelCC/SynergyNetServer/Server/Data/client_connections"
)

func HandleConnection(client *client_socket.ClientSocket, clients *client_socket.ClientSlice) {

	defer client.Conn.Close()
	connected := make(chan bool, 1)
	connected <- true
	println("Cliente conectado: ", client.Host)
	go client.ReadData(connected)
	go client.SendData()

	select {
	case <-connected:
		if !<-connected {
			fmt.Println("Cliente desconectado: ", client.NameClient)
			clients.RemoveClient(client.Host)
			fmt.Println("Cerrando conexion")
			return
		}

	default:
		fmt.Println("Cliente desconectado")
	}

}
