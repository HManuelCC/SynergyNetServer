package soketserver

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	client_socket "github.com/HManuelCC/SynergyNetServer/Server/Data/client_connections"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/request_response"
	"github.com/HManuelCC/SynergyNetServer/Server/handler_connections"
)

var clients *client_socket.ClientSlice = &client_socket.ClientSlice{}

func NewSocketServer(port int) {

	var conexionesActuales int = 0

	server, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Println("Error al crear el servidor: ", err)
		return
	}

	defer server.Close()

	log.Println("Servidor corriendo en el puerto: ", port)

	for {
		conn, err := server.Accept()

		conexionesActuales++

		if err != nil {
			log.Println("Error al aceptar la conexión: ", err)
			continue
		} else {
			var addr string = conn.RemoteAddr().String()
			var port string = ""
			port = strings.Split(conn.RemoteAddr().String(), ":")[1]
			fmt.Println("Obteniendo nombre del cliente")
			clientName, err := client_socket.GetCLientName(conn)
			if err != nil {
				log.Println("Error al obtener el nombre del cliente: ", err)
				conn.Close()
			} else {
				client := &client_socket.ClientSocket{NameClient: clientName, Port: port, Conn: conn, Host: addr, Events: make(chan request_response.Event), States: make(chan request_response.State)}
				*clients = append(*clients, client)

				log.Println("Nueva conexión establecida: " + client.NameClient)
				go handler_connections.HandleConnection(client, clients)
			}

		}

	}
}
