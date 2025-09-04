package SynergyNetServer

import (
	"crypto/tls"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/balancer"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
	"github.com/HManuelCC/SynergyNetServer/Server/handler_connections"
)

var Clients *client.ClientSliceGroups = &client.ClientSliceGroups{}

func NewSocketServer(port int) {

	var conexionesActuales int = 0

	cert, err := tls.LoadX509KeyPair("../Certs/server.crt", "../Certs/private_server.key")
	if err != nil {
		log.Fatal("Error cargando certificado:", err)
	}

	// Configuración TLS
	config := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS13}

	server, err := tls.Listen("tcp", ":"+strconv.Itoa(port), config)
	if err != nil {
		log.Println("Error al crear el servidor: ", err)
		return
	}
	// Iniciar las colas y lanzamos 5 goroutines para cada una
	balancer.BalancerEventQueue.Start(Clients, 1000, 5)
	balancer.BalancerStatesQueue.Start(Clients, 1000, 5)
	balancer.BalancerErrorEventQueue.Start(Clients, 1000, 5)
	balancer.BalancerErrorStatesQueue.Start(Clients, 100, 5)
	// Detener las colas
	defer balancer.BalancerEventQueue.Stop()
	defer balancer.BalancerStatesQueue.Stop()
	// Detener la cola de errores
	defer balancer.BalancerErrorEventQueue.Stop()
	defer balancer.BalancerErrorStatesQueue.Stop()

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
			fmt.Println("Obteniendo información del cliente")
			clientInfo, err := client.ConnectAndGetInfo(conn)
			if err != nil || clientInfo == nil {
				log.Println("Error al obtener la información del cliente: ", err)
				conn.Close()
			} else {
				clientInfo.ClientName = strings.ToUpper(clientInfo.ClientName)
				client := &client.ClientSocket{Info: *clientInfo, Port: port, Conn: conn, Host: addr, Events: make(chan comunication.Event), States: make(chan comunication.State)}
				Clients.AddClientToGroup(client)
				log.Println("Nueva conexión establecida: " + client.Info.ClientName)
				go handler_connections.HandleConnection(client, Clients)
			}

		}

	}

}
